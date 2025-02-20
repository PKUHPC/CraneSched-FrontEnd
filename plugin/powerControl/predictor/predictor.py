import sys
import torch
import yaml
import numpy as np
import pandas as pd
import joblib
import holidays
from flask import Flask, jsonify
from influxdb_client import InfluxDBClient
from datetime import timedelta

from model import NodePredictorNN


def load_config():
    if len(sys.argv) < 2:
        raise RuntimeError("Config path not provided as command line argument")

    config_path = sys.argv[1]
    print(f"Loading config from {config_path}...")
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        raise RuntimeError(f"Failed to load config from {config_path}: {str(e)}")


CONFIG = load_config()
CHECKPOINT_PATH = CONFIG["Predictor"]["CheckpointPath"]
SCALERS_PATH = CONFIG["Predictor"]["ScalersPath"]

INFLUX_CONFIG = {
    "url": CONFIG["InfluxDB"]["URL"],
    "token": CONFIG["InfluxDB"]["Token"],
    "org": CONFIG["InfluxDB"]["Org"],
    "bucket": CONFIG["InfluxDB"]["Bucket"],
}

PREDICTION_CONFIG = {
    "cpus_per_node": CONFIG["Predictor"]["CPUsPerNode"],
    "forecast_minutes": CONFIG["Predictor"]["ForecastMinutes"],
    "lookback_minutes": CONFIG["Predictor"]["LookbackMinutes"],
}


class NodePredictor:
    def __init__(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.cn_holidays = holidays.CN()

        self.influx_client = InfluxDBClient(
            url=INFLUX_CONFIG["url"],
            token=INFLUX_CONFIG["token"],
            org=INFLUX_CONFIG["org"],
        )

        self._init_model()

    def _init_model(self):
        self.model = NodePredictorNN(feature_size=5).to(self.device)

        try:
            print(f"Loading model from {CHECKPOINT_PATH}...")

            checkpoint = torch.load(
                CHECKPOINT_PATH, map_location=self.device, weights_only=True
            )

            print("Checkpoint keys:", checkpoint.keys())

            if "model_state_dict" in checkpoint:
                model_state = checkpoint["model_state_dict"]
                print("Model state dict keys:", model_state.keys())
                self.model.load_state_dict(model_state, strict=False)
            else:
                print("Loading state dict directly...")
                self.model.load_state_dict(checkpoint, strict=False)

            print("Model loaded successfully, loading scalers...")

            scalers = joblib.load(SCALERS_PATH)
            self.feature_scaler = scalers["feature_scaler"]
            self.target_scaler = scalers["target_scaler"]
            self.dayback_scaler = scalers["dayback_scaler"]

            print("Scalers loaded successfully")

        except Exception as e:
            print(f"Model loading error: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            import traceback

            print("Full traceback:")
            print(traceback.format_exc())
            raise RuntimeError(f"Failed to load model or scalers: {str(e)}")

        self.model.eval()

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

            print(f"Query successful, shape: {result.shape}")
            print("Columns:", result.columns.tolist())

            required_columns = [
                "_time",
                "minute_time",
                "node_id",
                "task_count",
                "req_cpu_ratio",
                "avg_req_cpu_per_task",
                "avg_task_runtime",
            ]

            for col in required_columns:
                if col not in result.columns:
                    raise ValueError(f"Missing required column: {col}")

            cluster_metrics = result.groupby("minute_time", as_index=False).agg(
                {
                    "task_count": "sum",
                    "req_cpu_ratio": "mean",
                    "avg_req_cpu_per_task": "mean",
                    "avg_task_runtime": "mean",
                }
            )

            print("\n=== cluster_metrics info ===")
            print(f"Shape: {cluster_metrics.shape}")
            print("cluster_metrics minute_time values:")
            print(cluster_metrics["minute_time"].head())

            active_nodes_df = result[result["task_count"] > 0]
            print("\n=== active nodes data ===")
            print("Active nodes data shape:", active_nodes_df.shape)
            print("Active nodes minute_time values:")
            print(active_nodes_df["minute_time"].unique())

            active_nodes = (
                result[result["task_count"] > 0]
                .groupby("minute_time")["node_id"]
                .nunique()
                .reindex(cluster_metrics["minute_time"])
                .fillna(0)
            )

            print("\n=== active_nodes info ===")
            print("active_nodes shape:", active_nodes.shape)
            print("active_nodes values:")
            print(active_nodes.head())

            df = pd.DataFrame()
            df["datetime"] = pd.to_datetime(cluster_metrics["minute_time"], unit="s")
            df["active_node_count"] = active_nodes.values
            df["waiting_job_count"] = 0
            df["running_job_count"] = cluster_metrics["task_count"].fillna(0)
            df["avg_req_cpu_occupancy_rate"] = cluster_metrics["req_cpu_ratio"].fillna(0)
            df["avg_req_cpu_per_job"] = cluster_metrics["avg_req_cpu_per_task"].fillna(0)
            df["avg_runtime_minutes"] = cluster_metrics["avg_task_runtime"].fillna(0)

            print("\n=== final DataFrame info ===")
            print(f"Shape: {df.shape}")
            print("Final data:")
            print(df.head())

            return df[
                [
                    "datetime",
                    "running_job_count",
                    "waiting_job_count",
                    "active_node_count",
                    "avg_req_cpu_occupancy_rate",
                    "avg_req_cpu_per_job",
                    "avg_runtime_minutes",
                ]
            ]

        except Exception as e:
            print(f"Query error: {str(e)}")
            raise

    def predict(self):
        try:
            df = self._query_influx()

            past_hour_data, cur_datetime, dayback_data = self._process_data(df)

            print("Data dimensions:")
            print(f"past_hour_data shape: {past_hour_data.shape}")
            print(f"cur_datetime shape: {len(cur_datetime)}")
            print(f"dayback_data shape: {dayback_data.shape}")

            past_hour_tensor = (
                torch.FloatTensor(past_hour_data).unsqueeze(0).to(self.device)
            )
            cur_datetime_tensor = (
                torch.FloatTensor(cur_datetime).unsqueeze(0).to(self.device)
            )
            dayback_tensor = (
                torch.FloatTensor(dayback_data).unsqueeze(0).to(self.device)
            )

            print("\nTensor dimensions:")
            print(f"past_hour_tensor shape: {past_hour_tensor.shape}")
            print(f"cur_datetime_tensor shape: {cur_datetime_tensor.shape}")
            print(f"dayback_tensor shape: {dayback_tensor.shape}")

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
            print(f"Prediction error: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            import traceback

            print("Full traceback:")
            print(traceback.format_exc())
            return 0

    def _process_data(self, df):
        feature_names = [
            "running_job_count",
            "active_node_count",
            "avg_req_cpu_occupancy_rate",
            "avg_req_cpu_per_job",
            "avg_runtime_minutes",
        ]

        past_hour_features = df[feature_names].values[
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

        past_hour_features = self.feature_scaler.transform(past_hour_features)
        dayback_features = self.dayback_scaler.transform(
            dayback_features.reshape(1, -1)
        )

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

        return [
            is_weekend,
            is_holiday,
            main_period / 6.0,
        ]

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
        """Check if date is holiday"""
        return date in self.cn_holidays

    def _get_dayback_features(self, df, current_idx, target_col):
        pattern_features = []
        window_minutes = 30

        for days_back in [1, 3, 5, 7]:
            minutes_back = days_back * 24 * 60
            historical_center_idx = current_idx - minutes_back

            if (
                historical_center_idx >= window_minutes
                and historical_center_idx + window_minutes < len(df)
            ):
                historical_window = df[target_col].iloc[
                    historical_center_idx
                    - window_minutes : historical_center_idx
                    + window_minutes
                ]

                pattern_features.extend(
                    [
                        float(historical_window.min()),
                        float(historical_window.max()),
                    ]
                )
            else:
                current_value = float(df[target_col].iloc[current_idx])
                pattern_features.extend([current_value] * 2)

        return np.array(pattern_features, dtype=np.float32)


app = Flask(__name__)
predictor = NodePredictor()

@app.route("/predict", methods=["POST"])
def predict_nodes():
    try:
        prediction = predictor.predict()
        return jsonify({"prediction": prediction})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
