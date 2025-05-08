import os
import yaml
import logging
import holidays
import traceback
import argparse
import math

import torch
import joblib
import numpy as np
import pandas as pd

from flask import Flask, jsonify, request
from influxdb_client import InfluxDBClient
from datetime import timedelta
from waitress import serve
from urllib.parse import urlparse

from model import NodePredictorNN


def load_config():
    """Load configuration from YAML file and setup logging"""
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
        
        log_dir = os.path.dirname(config["Predictor"]["PredictorLogFile"])
        if log_dir: 
            os.makedirs(log_dir, mode=0o755, exist_ok=True)
            
        logging.basicConfig(
            filename=config["Predictor"]["PredictorLogFile"],
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        logging.info(f"Loading config from {args.config}...")
        return config
    except Exception as e:
        raise RuntimeError(f"Failed to load config from {args.config}: {str(e)}")


CONFIG = load_config()

DEBUG = CONFIG["Predictor"]["Debug"]
PORT = urlparse(CONFIG["Predictor"]["URL"]).port or 5000

CHECKPOINT_PATH = CONFIG["Predictor"]["CheckpointFile"]
SCALERS_PATH = CONFIG["Predictor"]["ScalersFile"]
FORECAST_MINUTES = CONFIG["Predictor"]["ForecastMinutes"]
LOOKBACK_MINUTES = CONFIG["Predictor"]["LookbackMinutes"]


INFLUX_CONFIG = {
    "url": CONFIG["InfluxDB"]["URL"],
    "token": CONFIG["InfluxDB"]["Token"],
    "org": CONFIG["InfluxDB"]["Org"],
    "bucket": CONFIG["InfluxDB"]["Bucket"],
}


class NodePredictor:
    def __init__(self):
        """Initialize the predictor with model, device and configurations"""
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.feature_names = [
            'running_job_count',
            'active_node_count',
            'avg_req_cpu_rate',
            'avg_req_cpu_per_job',
            'avg_runtime_minutes',
        ]

        self.feature_size = len(self.feature_names)
        self.influx_client = InfluxDBClient(
            url=INFLUX_CONFIG["url"],
            token=INFLUX_CONFIG["token"],
            org=INFLUX_CONFIG["org"],
        )

        self._init_model()

    def _init_model(self):
        """Initialize neural network model and load checkpoints and scalers"""
        self.model = NodePredictorNN(feature_size=self.feature_size).to(self.device)

        try:
            checkpoint = torch.load(
                CHECKPOINT_PATH, map_location=self.device, weights_only=True
            )

            if "model_state_dict" in checkpoint:
                logging.info("Loading model state dict from checkpoint")
                model_state = checkpoint["model_state_dict"]
                self.model.load_state_dict(model_state, strict=False)
            else:
                logging.error("No model state dict found in checkpoint")
                raise RuntimeError("No model state dict found in checkpoint")

            self.model.eval()

            logging.info("Loading scalers from path: %s", SCALERS_PATH)
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

    def _query_influx(self):
        """Query recent cluster metrics from InfluxDB"""
        query = f"""
        from(bucket: "{INFLUX_CONFIG['bucket']}")
            |> range(start: -{LOOKBACK_MINUTES + 60}m)
            |> pivot(rowKey: ["_time", "node_id"], columnKey: ["_field"], valueColumn: "_value")
        """

        try:
            result = self.influx_client.query_api().query_data_frame(query)
            if result.empty:
                raise ValueError("No data found in InfluxDB")

            logging.debug(f"Query successful, shape: {result.shape}")

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

            logging.debug("\n=== cluster_metrics info ===")
            logging.debug(f"cluster_metrics: {cluster_metrics.head()}")
            logging.debug(f"cluster_metrics: {cluster_metrics.tail()}")
            logging.debug(f"Shape: {cluster_metrics.shape}")

            active_nodes = (
                result[result["job_count"] > 0]
                .groupby("minute_time")["node_id"]
                .nunique()
                .rolling(window=2, min_periods=1)  # Use 2-minute rolling window
                .max()                             # Take max value in the window
                .reindex(cluster_metrics["minute_time"])
                .fillna(0)
            )

            logging.debug("\n=== active_nodes info ===")
            logging.debug(f"active_nodes shape: {active_nodes.shape}")
            logging.debug(f"Current active nodes: {active_nodes.values[-1]}")

            df = pd.DataFrame()
            df["datetime"] = pd.to_datetime(cluster_metrics["minute_time"], unit="s").dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
            df["active_node_count"] = active_nodes.values
            df["running_job_count"] = cluster_metrics["job_count"].fillna(0)
            df["avg_req_cpu_rate"] = cluster_metrics["req_cpu_rate"].fillna(0)
            df["avg_req_cpu_per_job"] = cluster_metrics["avg_req_cpu_per_job"].fillna(0)
            df["avg_runtime_minutes"] = cluster_metrics["avg_job_runtime"].fillna(0)

            return df

        except Exception as e:
            logging.error(f"Query error: {str(e)}")
            raise

    def _query_historical_point(self, target_time):
        """Query cluster state data for a specific historical time point"""
        query = f"""
        from(bucket: "{INFLUX_CONFIG['bucket']}")
            |> range(start: {int(target_time.timestamp())}s, stop: {int(target_time.timestamp()) + 300}s)
            |> pivot(rowKey: ["_time", "node_id"], columnKey: ["_field"], valueColumn: "_value")
        """
        
        try:
            result = self.influx_client.query_api().query_data_frame(query)
            if result.empty:
                return None
                
            active_nodes = (
                result[result["job_count"] > 0]
                .groupby("minute_time")["node_id"]
                .nunique()
                .mean()
            )
            
            return active_nodes
            
        except Exception as e:
            logging.error(f"Historical query error: {str(e)}")
            raise

    def predict(self, total_nodes):
        """Make prediction for required number of nodes"""
        try:
            logging.info("\n=== Starting Prediction Process ===")
            
            df = self._query_influx()
            
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', None)
            
            logging.debug("\nOriginal Data Overview:")
            logging.debug(f"DataFrame shape: {df.shape}")
            logging.debug(f"DataFrame head:\n{df.head()}")
            logging.debug(f"DataFrame tail:\n{df.tail()}")
            
            logging.info("\nData Quality Check:")
            logging.info(f"Contains null values: {df.isnull().any().any()}")
            if df.isnull().any().any():
                logging.info("Null value statistics:\n" + df.isnull().sum().to_string())

            df["active_node_count"] = df["active_node_count"] / total_nodes

            past_hour_data, cur_datetime, dayback_data = self._process_data(df)

            logging.debug("\nProcessed Data Dimensions:")
            logging.debug(f"past_hour_data shape: {past_hour_data.shape}")
            logging.debug(f"past_hour_data range: [{past_hour_data.min()}, {past_hour_data.max()}]")
            logging.debug(f"past_hour_data samples:\n{past_hour_data[:6]}")
            logging.debug(f"\ncur_datetime shape: {cur_datetime.shape}")
            logging.debug(f"cur_datetime value: {cur_datetime}")
            logging.debug(f"\ndayback_data shape: {dayback_data.shape}")
            logging.debug(f"dayback_data value: {dayback_data}")

            self.model.eval()
            with torch.no_grad():
                past_hour_tensor = torch.FloatTensor(past_hour_data).unsqueeze(0).to(self.device)
                cur_datetime_tensor = torch.FloatTensor(cur_datetime).unsqueeze(0).to(self.device)
                dayback_tensor = torch.FloatTensor(dayback_data).unsqueeze(0).to(self.device)
                
                logging.debug("\nTensor Dimensions and Values:")
                logging.debug(f"past_hour_tensor shape: {past_hour_tensor.shape}")
                logging.debug(f"past_hour_tensor range: [{past_hour_tensor.min().item()}, {past_hour_tensor.max().item()}]")
                logging.debug(f"\ncur_datetime_tensor shape: {cur_datetime_tensor.shape}")
                logging.debug(f"cur_datetime_tensor value: {cur_datetime_tensor}")
                logging.debug(f"\ndayback_tensor shape: {dayback_tensor.shape}")
                logging.debug(f"dayback_tensor value: {dayback_tensor}")
                
                logging.info("\nStarting Model Prediction")
                prediction_scaled = self.model(
                    past_hour_tensor, cur_datetime_tensor, dayback_tensor
                )
                
                logging.debug(f"Scaled prediction shape: {prediction_scaled.shape}")
                logging.debug(f"Scaled prediction value: {prediction_scaled.cpu().numpy()}")
                
                prediction = self.target_scaler.inverse_transform(
                    prediction_scaled.cpu().numpy()
                )
                logging.debug(f"After inverse transform: {prediction}")

                prediction_scaled_by_nodes = prediction * total_nodes
                logging.debug(f"After scaling by total nodes ({total_nodes}): {prediction_scaled_by_nodes}")

                final_prediction = max(0, math.ceil(float(prediction_scaled_by_nodes[0][0])))
                logging.debug(f"Final prediction (after ceiling): {final_prediction}")
                
                return final_prediction

        except Exception as e:
            logging.error("\n=== Prediction Process Error ===")
            logging.error(f"Error type: {type(e).__name__}")
            logging.error(f"Error message: {str(e)}")
            logging.error("Full traceback:")
            logging.error(traceback.format_exc())
            return 0

    def _process_data(self, df):
        """Process raw data into model input format"""
        required_minutes = LOOKBACK_MINUTES
        if len(df) < required_minutes:
            logging.warning(f"Insufficient data points after aggregation. Required: {required_minutes}, Got: {len(df)}")
            first_row = df.iloc[0].copy()
            padding_size = required_minutes - len(df)
            padding_df = pd.DataFrame([first_row] * padding_size)
            padding_df['datetime'] = pd.date_range(
                end=df['datetime'].iloc[0] - timedelta(minutes=1),
                periods=padding_size,
                freq='-1min'
            )
            df = pd.concat([padding_df, df], ignore_index=True)
            logging.info(f"Data padded to {len(df)} records")

        past_hour_features = df[self.feature_names].values[-required_minutes:]
        past_hour_features = self.feature_scaler.transform(past_hour_features)
        
        cur_time = df["datetime"].iloc[-1]
        cur_datetime_features = self._create_time_features(
            cur_time,
            cur_time + timedelta(minutes=FORECAST_MINUTES),
        )

        dayback_features = self._get_dayback_features(
            df, "active_node_count"
        )

        return past_hour_features, cur_datetime_features, dayback_features

    def _create_time_features(self, start_time, end_time):
        """Create time-related features for the model"""
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
        """Convert hour to day period index"""
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
        """Check if given date is a Chinese holiday"""
        return date in holidays.CN()

    def _get_dayback_features(self, df, target_col):
        """Get historical features from previous days"""
        current_idx = len(df) - 1
        current_time = df["datetime"].iloc[-1]
        current_value = float(df[target_col].iloc[current_idx])
        dayback_features = []

        for days_back in [1, 3, 5, 7]:
            target_time = current_time - timedelta(days=days_back)
            historical_value = self._query_historical_point(target_time)
            value = historical_value if historical_value is not None else current_value
            dayback_features.append(value)

        dayback_features = np.array(dayback_features, dtype=np.float32)
        
        logging.debug(f"Values before normalization: {dayback_features}")
        dayback_features = self.dayback_scaler.transform(
            dayback_features.reshape(1, -1)
        )
        
        return dayback_features[0]


app = Flask(__name__)
predictor = NodePredictor()


@app.route("/predict", methods=["POST"])
def predict_nodes():
    """Flask endpoint for node prediction"""
    try:
        request_data = request.get_json()
        if request_data is None:
            return jsonify({"error": "Request body must be JSON"}), 400
            
        total_nodes = request_data.get("total_nodes", 0)
        prediction = predictor.predict(total_nodes)
        
        return jsonify({"prediction": prediction})
    except Exception as e:
        logging.error(f"Prediction error: {str(e)}")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error("Full traceback:")
        logging.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint to verify service is running"""
    try:
        if predictor is None or predictor.model is None:
            logging.error("Predictor not initialized")
            return "", 500
            
        if predictor.influx_client is None:
            logging.error("InfluxDB client not initialized")
            return "", 500
            
        logging.info("Health check passed")
        return "", 200
        
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        return "", 500


if __name__ == "__main__":
    if DEBUG:
        app.run(host="0.0.0.0", port=PORT, debug=True)
    else:
        serve(app, host="0.0.0.0", port=PORT)
