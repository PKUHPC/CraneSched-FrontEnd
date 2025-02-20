import sys
import torch
import yaml
import numpy as np
import pandas as pd
import joblib
import holidays
import traceback
from flask import Flask, jsonify, request
from influxdb_client import InfluxDBClient
from datetime import timedelta
import argparse
from waitress import serve
from urllib.parse import urlparse
import logging

from model import NodePredictorNN


def load_config():
    parser = argparse.ArgumentParser(
        description='Node predictor service',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '-c', '--config',
        default='config.yaml',
        help='Path to configuration file'
    )

    args = parser.parse_args()
    
    try:
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)
            
        logging.basicConfig(
            filename=config["Predictor"]["LogFile"],
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        logging.info(f"Loading config from {args.config}...")
        return config
    except Exception as e:
        raise RuntimeError(f"Failed to load config from {args.config}: {str(e)}")


CONFIG = load_config()

logging.info(f"CONFIG: {CONFIG}")

DEBUG = CONFIG["Predictor"]["Debug"]
url = urlparse(CONFIG["Predictor"]["URL"])
PORT = url.port or 5000

CHECKPOINT_PATH = CONFIG["Predictor"]["CheckpointPath"]
SCALERS_PATH = CONFIG["Predictor"]["ScalersPath"]

INFLUX_CONFIG = {
    "url": CONFIG["InfluxDB"]["URL"],
    "token": CONFIG["InfluxDB"]["Token"],
    "org": CONFIG["InfluxDB"]["Org"],
    "bucket": CONFIG["InfluxDB"]["Bucket"],
}

# 这里的cpu个数很突兀
PREDICTION_CONFIG = {
    "cpus_per_node": 32,
    "forecast_minutes": CONFIG["Predictor"]["ForecastMinutes"],
    "lookback_minutes": CONFIG["Predictor"]["LookbackMinutes"],
}


class NodePredictor:
    def __init__(self):
        # 需要安装pytorch环境
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.influx_client = InfluxDBClient(
            url=INFLUX_CONFIG["url"],
            token=INFLUX_CONFIG["token"],
            org=INFLUX_CONFIG["org"],
        )

        self._init_model()

    def _init_model(self):
        # 这里的feature_size需要和模型中的feature_size一致，而且直接传个数字进去有些不好看
        self.model = NodePredictorNN(feature_size=6).to(self.device)

        try:
            checkpoint = torch.load(
                CHECKPOINT_PATH, map_location=self.device, weights_only=True
            )

            if "model_state_dict" in checkpoint:
                model_state = checkpoint["model_state_dict"]
                self.model.load_state_dict(model_state, strict=False)
            else:
                self.model.load_state_dict(checkpoint, strict=False)

            self.model.eval()

            scalers = joblib.load(SCALERS_PATH)
            self.feature_scaler = scalers["feature_scaler"]
            self.dayback_scaler = scalers["dayback_scaler"]

            self.target_scaler = scalers["target_scaler"]

        except Exception as e:
            logging.error(f"Model loading error: {str(e)}")
            logging.error(f"Error type: {type(e).__name__}")
            logging.error("Full traceback:")
            logging.error(traceback.format_exc())
            raise RuntimeError(f"Failed to load model or scalers: {str(e)}")

    # 这里要检查sql是否正确
    def _query_influx(self):
        query = f"""
        from(bucket: "{INFLUX_CONFIG['bucket']}")
            |> range(start: -{PREDICTION_CONFIG['lookback_minutes']}m)
            |> pivot(rowKey: ["_time", "node_id"], columnKey: ["_field"], valueColumn: "_value")
        """

        try:
            result = self.influx_client.query_api().query_data_frame(query)

            if result.empty:
                raise ValueError("No data found in InfluxDB")

            logging.info(f"Query successful, shape: {result.shape}")
            logging.info("Columns:", result.columns.tolist())

            required_columns = [
                "_time",
                "minute_time",
                "node_id",
                "job_count",
                "req_cpu_rate",
                "avg_req_cpu_per_job",
                "avg_job_runtime",
            ]

            for col in required_columns:
                if col not in result.columns:
                    raise ValueError(f"Missing required column: {col}")

            cluster_metrics = result.groupby("minute_time", as_index=False).agg(
                {
                    "job_count": "sum",
                    "req_cpu_rate": "mean",
                    "avg_req_cpu_per_job": "mean",
                    "avg_job_runtime": "mean",
                }
            )

            logging.info(f"cluster_metrics: {cluster_metrics}")

            logging.info("\n=== cluster_metrics info ===")
            logging.info(f"Shape: {cluster_metrics.shape}")

            active_nodes_df = result[result["job_count"] > 0]
            logging.info("\n=== active nodes data ===")
            logging.info(f"Active nodes data shape: {active_nodes_df.shape}")

            active_nodes = (
                result[result["job_count"] > 0]
                .groupby("minute_time")["node_id"]
                .nunique()
                .reindex(cluster_metrics["minute_time"])
                .fillna(0)
            )

            logging.info("\n=== active_nodes info ===")
            logging.info(f"active_nodes shape: {active_nodes.shape}")

            df = pd.DataFrame()
            df["datetime"] = pd.to_datetime(cluster_metrics["minute_time"], unit="s")
            df["active_node_count"] = active_nodes.values
            df["running_job_count"] = cluster_metrics["job_count"].fillna(0)
            df["avg_req_cpu_rate"] = cluster_metrics["req_cpu_rate"].fillna(
                0
            )
            df["avg_req_cpu_per_job"] = cluster_metrics["avg_req_cpu_per_job"].fillna(
                0
            )
            df["avg_runtime_minutes"] = cluster_metrics["avg_job_runtime"].fillna(0)

            logging.info(f"df: {df}")
            return df

        except Exception as e:
            logging.error(f"Query error: {str(e)}")
            raise

    def predict(self, avg_req_node_per_job, total_nodes):
        try:
            df = self._query_influx()
            df["avg_req_node_per_job"] = avg_req_node_per_job

            past_hour_data, cur_datetime, dayback_data = self._process_data(df, total_nodes)

            logging.info("Data dimensions:")
            logging.info(f"past_hour_data shape: {past_hour_data.shape}")
            logging.info(f"cur_datetime shape: {cur_datetime.shape}")
            logging.info(f"dayback_data shape: {dayback_data.shape}")

            past_hour_tensor = (
                torch.FloatTensor(past_hour_data).unsqueeze(0).to(self.device)
            )
            cur_datetime_tensor = (
                torch.FloatTensor(cur_datetime).unsqueeze(0).to(self.device)
            )
            dayback_tensor = (
                torch.FloatTensor(dayback_data).unsqueeze(0).to(self.device)
            )

            logging.info("\nTensor dimensions:")
            logging.info(f"past_hour_tensor shape: {past_hour_tensor.shape}")
            logging.info(f"cur_datetime_tensor shape: {cur_datetime_tensor.shape}")
            logging.info(f"dayback_tensor shape: {dayback_tensor.shape}")

            self.model.eval()
            with torch.no_grad():
                prediction_scaled = self.model(
                    past_hour_tensor, cur_datetime_tensor, dayback_tensor
                )

            prediction = self.target_scaler.inverse_transform(
                prediction_scaled.cpu().numpy()
            )

            final_prediction = max(0, round(float(prediction[0][0])))
            return final_prediction

        except Exception as e:
            logging.error(f"Prediction error: {str(e)}")
            logging.error(f"Error type: {type(e).__name__}")
            logging.error("Full traceback:")
            logging.error(traceback.format_exc())
            return 0

    def _process_data(self, df, total_nodes):
        past_hour_features_names = [
            "running_job_count",
            "active_node_count",
            "avg_req_cpu_rate",
            "avg_req_cpu_per_job",
            "avg_req_node_per_job",
            "avg_runtime_minutes",
        ]

        past_hour_features = df[past_hour_features_names].values[
            -PREDICTION_CONFIG["lookback_minutes"] :
        ]

        cur_time = df["datetime"].iloc[-1]
        cur_datetime_features = self._create_time_features(
            cur_time,
            cur_time + timedelta(minutes=PREDICTION_CONFIG["forecast_minutes"]),
        )

        current_idx = len(df) - 1
        dayback_features = self._get_dayback_features(
            df, current_idx, "active_node_count"
        )
        
        if total_nodes > 0:
            dayback_features = dayback_features / total_nodes
        else:
            dayback_features = np.zeros_like(dayback_features)
        
        dayback_features = np.clip(dayback_features, 0, 1)
        
        dayback_features = self.dayback_scaler.transform(
            dayback_features.reshape(1, -1)
        )

        past_hour_features = self.feature_scaler.transform(past_hour_features)

        return past_hour_features, cur_datetime_features, dayback_features[0]

    def _create_time_features(self, start_time, end_time):
        """Create time range feature vector"""
        is_weekend = float(start_time.dayofweek >= 5)
        is_holiday = float(self._is_holiday(start_time.date()))

        hours = pd.date_range(start_time, end_time, freq="1min").hour
        period_counts = np.zeros(6)
        for hour in hours:
            period = self._get_day_period(hour)
            period_counts[period] += 1

        main_period = np.argmax(period_counts)

        return np.array([
            is_weekend,
            is_holiday,
            main_period / 6.0,
        ], dtype=np.float32)

    def _get_day_period(self, hour):
        """Get period of the day"""
        if 5 <= hour < 9:
            return 0  # early morning
        elif 9 <= hour < 12:
            return 1  # morning
        elif 12 <= hour < 14:
            return 2  # noon
        elif 14 <= hour < 18:
            return 3  # afternoon
        elif 18 <= hour < 24:
            return 4  # evening
        else:
            return 5  # night

    def _is_holiday(self, date):
        return date in holidays.CN()

    def _get_dayback_features(self, df, current_idx, target_col):
        dayback_features = []

        for days_back in [1, 3, 5, 7]:
            minutes_back = days_back * 24 * 60
            historical_center_idx = current_idx - minutes_back

            if 0 <= historical_center_idx < len(df):
                dayback_features.append(
                    float(df[target_col].iloc[historical_center_idx])
                )
            else:
                dayback_features.append(
                    float(df[target_col].iloc[current_idx])
                )

        return np.array(dayback_features, dtype=np.float32)


app = Flask(__name__)
predictor = NodePredictor()


@app.route("/predict", methods=["POST"])
def predict_nodes():
    try:
        request_data = request.get_json()
        if request_data is None:
            return jsonify({"error": "Request body must be JSON"}), 400
            
        avg_req_node_per_job = request_data.get("avg_nodes_per_job", 0)
        total_nodes = request_data.get("total_nodes", 0)
        logging.info(f"Received prediction request with avg_req_node_per_job: {avg_req_node_per_job}, total_nodes: {total_nodes}")

        prediction = predictor.predict(avg_req_node_per_job, total_nodes)
        logging.info(f"Received prediction request with avg_req_node_per_job: {avg_req_node_per_job}, total_nodes: {total_nodes}")
        logging.info(f"Prediction: {prediction}")
        
        return jsonify({"prediction": prediction})
    except Exception as e:
        logging.error(f"Prediction error: {str(e)}")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error("Full traceback:")
        logging.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    if DEBUG:
        app.run(host="0.0.0.0", port=PORT, debug=True)
    else:
        serve(app, host="0.0.0.0", port=PORT)
