import os
import joblib
import holidays
import pandas as pd
import numpy as np
from config import MODEL_CONFIG
from sklearn.preprocessing import RobustScaler, MinMaxScaler

class DataProcessor:
    def __init__(self):
        self.config = MODEL_CONFIG
        
        # 1. 修改scaler的配置
        self.feature_scaler = RobustScaler(
            with_centering=True,
            with_scaling=True,
            quantile_range=(5.0, 95.0)  # 使用更宽的四分位范围
        )
        
        # 2. 修改目标值的scaler
        self.target_scaler = MinMaxScaler(
            feature_range=(0, 1)  # 改为0-1范围，因为是百分比
        )
        
        self.dayback_scaler = RobustScaler(
            with_centering=True,
            with_scaling=True,
            quantile_range=(5.0, 95.0)
        )
        
        self.cn_holidays = holidays.CN()
        
        self.pst_hour_feature_names = [
            'running_job_count',
            # 'waiting_job_count',
            # 'active_node_ratio',  # 改为使用比例
            'active_node_count',
            # 'utilization_rate',
            'avg_req_cpu_rate',
            'avg_req_node_per_job',
            'avg_req_cpu_per_job',
            'avg_runtime_minutes'
        ]
        
        self.feature_size = len(self.pst_hour_feature_names)
        data_path = self.config['data_path']
        data_filename = os.path.splitext(os.path.basename(data_path))[0]
        self.dataset_dir = os.path.join(os.path.dirname(data_path), data_filename)
        os.makedirs(self.dataset_dir, exist_ok=True)

    def process_and_save_data(self):
        """处理数据并保存"""
        print("开始处理数据...")
        
        # 加载原始数据
        data = pd.read_csv(self.config['data_path'])
        data = data.sort_values('datetime').reset_index(drop=True)
        
        # 准备时间序列数据
        past_hour_features, cur_datetime_features, dayback_features, target_values = \
            self.prepare_time_series_data(data)
        
        # 确保目标值非负
        target_values = np.maximum(target_values, 0)
        
        # 划分数据集
        train_size = int(len(target_values) * 0.7)
        val_size = int(len(target_values) * 0.15)
        
        # 创建数据字典
        data_dict = {
            'X_train': [
                past_hour_features[:train_size],
                cur_datetime_features[:train_size],
                dayback_features[:train_size]
            ],
            'y_train': target_values[:train_size],
            
            'X_val': [
                past_hour_features[train_size:train_size + val_size],
                cur_datetime_features[train_size:train_size + val_size],
                dayback_features[train_size:train_size + val_size]
            ],
            'y_val': target_values[train_size:train_size + val_size],
            
            'X_test': [
                past_hour_features[train_size + val_size:],
                cur_datetime_features[train_size + val_size:],
                dayback_features[train_size + val_size:]
            ],
            'y_test': target_values[train_size + val_size:]
        }
        
        # 特征缩放
        for split in ['train', 'val', 'test']:
            if split == 'train':
                # 在训练集上拟合并转换
                data_dict[f'X_{split}'][0] = self.feature_scaler.fit_transform(
                    data_dict[f'X_{split}'][0].reshape(-1, self.feature_size)
                ).reshape(data_dict[f'X_{split}'][0].shape)
                
                data_dict[f'X_{split}'][2] = self.dayback_scaler.fit_transform(
                    data_dict[f'X_{split}'][2]
                )
                
                data_dict[f'y_{split}'] = self.target_scaler.fit_transform(
                    data_dict[f'y_{split}'].reshape(-1, 1)
                )
            else:
                # 在验证集和测试集上只进行转换
                data_dict[f'X_{split}'][0] = self.feature_scaler.transform(
                    data_dict[f'X_{split}'][0].reshape(-1, self.feature_size)
                ).reshape(data_dict[f'X_{split}'][0].shape)
                
                data_dict[f'X_{split}'][2] = self.dayback_scaler.transform(
                    data_dict[f'X_{split}'][2]
                )
                
                data_dict[f'y_{split}'] = self.target_scaler.transform(
                    data_dict[f'y_{split}'].reshape(-1, 1)
                )
        
        # 保存处理好的数据集
        self.save_processed_data(data_dict)
        print("数据处理完成并已保存")

    def prepare_time_series_data(self, df):
        """准备时间序列数据"""
        # 1. 添加数据验证
        # self._validate_input_data(df)
        
        # 2. 处理异常值
        # df = self._handle_outliers(df)
        
        # 3. 将active_node_count转换为占比
        # total_nodes = self.config['total_nodes']
        # df['active_node_ratio'] = df['active_node_count'] / total_nodes
        
        lookback = self.config['lookback_minutes']
        forecast_horizon = self.config['forecast_minutes']
        
        past_hour_sequences = []
        cur_datetime_feature_vectors = []
        dayback_feature_vectors = []
        target_values = []
        
        timestamps = pd.to_datetime(df['datetime'])
        
        for i in range(len(df) - lookback - forecast_horizon + 1):
            past_hour_data = df[self.pst_hour_feature_names].iloc[i:(i + lookback)].values
            
            target_start = i + lookback
            target_end = target_start + forecast_horizon
            target_value = df['active_node_count'].iloc[target_end-1]
            
            # 3. 生成时间特征
            cur_datetime_features = self._create_time_features(
                timestamps[target_start], 
                timestamps[target_end - 1]
            )
            
            # 4. 获取历史模式特征
            dayback_features = self._get_dayback_features(
                df, target_start, 'active_node_count'
            )
            
            past_hour_sequences.append(past_hour_data)
            cur_datetime_feature_vectors.append(cur_datetime_features)
            dayback_feature_vectors.append(dayback_features)
            target_values.append(target_value)
        
        return (np.array(past_hour_sequences),
                np.array(cur_datetime_feature_vectors),
                np.array(dayback_feature_vectors),
                np.array(target_values).reshape(-1, 1))

    def _create_time_features(self, start_time, end_time):
        """创建时间范围的特征向量"""
        is_weekend = float(start_time.dayofweek >= 5)
        is_holiday = float(self.is_holiday(start_time.date()))
        
        hours = pd.date_range(start_time, end_time, freq='1min').hour
        period_counts = np.zeros(6)
        for hour in hours:
            period = self.get_day_period(hour)
            period_counts[period] += 1
        
        main_period = np.argmax(period_counts)
        
        return [
            is_weekend,         # 是否周末 (0/1)
            is_holiday,         # 是否节假日 (0/1)
            main_period / 6.0,  # 主要时间段 (归一化到 0-1)
        ]

    def get_day_period(self, hour):
        """将一天分为不同时段"""
        if 5 <= hour < 9:
            return 0  # 早晨
        elif 9 <= hour < 12:
            return 1  # 上午
        elif 12 <= hour < 14:
            return 2  # 中午
        elif 14 <= hour < 18:
            return 3  # 下午
        elif 18 <= hour < 24:
            return 4  # 晚上
        else:
            return 5  # 深夜

    def is_holiday(self, date):
        """判断是否为节假日"""
        return date in self.cn_holidays

    def _get_dayback_features(self, df, current_idx, target_col):
        """获取历史模式特征"""
        pattern_features = []
        total_nodes = self.config['total_nodes']  # 需要在配置中添加总节点数
        
        for days_back in [1, 3, 5, 7]:
            minutes_back = days_back * 24 * 60
            historical_center_idx = current_idx - minutes_back
            
            if 0 <= historical_center_idx < len(df):
                # 将历史节点数也转换为占比
                pattern_features.append(float(df[target_col].iloc[historical_center_idx]) / total_nodes)
            else:
                current_value = float(df[target_col].iloc[current_idx]) / total_nodes
                pattern_features.append(current_value)
        
        return np.array(pattern_features, dtype=np.float32)

    def _validate_input_data(self, df):
        """验证输入数据的完整性和有效性"""
        # 检查必要的列是否存在
        required_columns = self.pst_hour_feature_names + ['datetime']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"缺少必要的列: {missing_columns}")
        
        # 检查时间戳的连续性并输出不连续点
        timestamps = pd.to_datetime(df['datetime'])
        time_diff = timestamps.diff()
        expected_diff = pd.Timedelta(minutes=1)
        
        discontinuous_points = time_diff[time_diff != expected_diff].index
        if len(discontinuous_points) > 0:
            print("\n=== 检测到时间序列不连续点 ===")
            for idx in discontinuous_points:
                if idx > 0:  # 跳过第一个点（因为它的diff是NaT）
                    print(f"不连续点位置: {idx}")
                    print(f"前一时间点: {timestamps[idx-1]}")
                    print(f"当前时间点: {timestamps[idx]}")
                    print(f"时间间隔: {time_diff[idx]}")
                    print("---")
            print(f"总计发现 {len(discontinuous_points)-1} 个不连续点")  # -1是为了排除第一个点
        
        # 检查数值的有效性
        for col in self.pst_hour_feature_names + ['active_node_count']:
            if df[col].isnull().any():
                null_count = df[col].isnull().sum()
                print(f"警告: 列 {col} 存在 {null_count} 个空值")
                # 输出空值的位置
                null_indices = df[df[col].isnull()].index
                print(f"空值位置: {null_indices.tolist()}")
            
            if (df[col] < 0).any():
                neg_count = (df[col] < 0).sum()
                print(f"警告: 列 {col} 存在 {neg_count} 个负值")
                # 输出负值的位置
                neg_indices = df[df[col] < 0].index
                print(f"负值位置: {neg_indices.tolist()}")

    def _handle_outliers(self, df):
        """处理异常值"""
        df_clean = df.copy()
        
        for col in self.pst_hour_feature_names:
            if col in ['running_job_count', 'waiting_job_count', 'active_node_count']:
                # 对于作业数量，只检查负值
                outliers = df[col] < 0
                if outliers.any():
                    print(f"列 {col} 发现 {outliers.sum()} 个负值")
                    # 将负值设为0
                    df_clean.loc[outliers, col] = 0
                    
            # elif col == 'avg_req_cpu_occupancy_rate':
            #     # 对于利用率，检查是否在合理范围内
            #     outliers = (df[col] < 0) | (df[col] > 100)
            #     if outliers.any():
            #         print(f"列 {col} 发现 {outliers.sum()} 个异常值")
            #         # 使用移动平均替换异常值
            #         window_size = 5
            #         moving_avg = df[col].rolling(
            #             window=window_size,
            #             center=True,
            #             min_periods=1
            #         ).mean()
            #         df_clean.loc[outliers, col] = moving_avg[outliers]
            
            print(f"列 {col} 的范围: [{df_clean[col].min()}, {df_clean[col].max()}]")
        
        return df_clean

    def save_processed_data(self, data_dict: dict, prefix: str = 'dataset'):
        """保存处理好的数据集和缩放器"""
        try:
            # 保存数据集
            for key, data in data_dict.items():
                if isinstance(data, list):
                    for i, feature_data in enumerate(data):
                        filename = f"{prefix}_{key}_part{i}.npy"
                        filepath = os.path.join(self.dataset_dir, filename)
                        np.save(filepath, feature_data)
                else:
                    filename = f"{prefix}_{key}.npy"
                    filepath = os.path.join(self.dataset_dir, filename)
                    np.save(filepath, data)
            
            # 保存缩放器
            scalers = {
                'feature_scaler': self.feature_scaler,
                'target_scaler': self.target_scaler,
                'dayback_scaler': self.dayback_scaler
            }
            scaler_path = os.path.join(self.dataset_dir, f"{prefix}_scalers.pkl")
            joblib.dump(scalers, scaler_path)
            
            print(f"数据集和缩放器已保存到: {self.dataset_dir}")
            
        except Exception as e:
            raise RuntimeError(f"保存数据集时出错: {str(e)}")

    def process_and_save_as_test_data(self):
        """处理数据并将整个数据集保存为测试集"""
        print("开始处理数据为测试集...")
        
        try:
            scaler_path = self.config['scaler_path']
            if not os.path.exists(scaler_path):
                raise FileNotFoundError(f"找不到 scaler 文件: {scaler_path}")
            
            scalers = joblib.load(scaler_path)
            self.feature_scaler = scalers['feature_scaler']
            self.target_scaler = scalers['target_scaler']
            self.dayback_scaler = scalers['dayback_scaler']
            print(f"成功从 {scaler_path} 加载数据缩放器")
        except Exception as e:
            raise RuntimeError(f"加载数据缩放器时出错: {str(e)}")
        
        # 加载原始数据
        data = pd.read_csv(self.config['data_path'])
        data = data.sort_values('datetime').reset_index(drop=True)
        
        # 准备时间序列数据
        past_hour_features, cur_datetime_features, dayback_features, target_values = \
            self.prepare_time_series_data(data)
        
        # 确保目标值非负
        target_values = np.maximum(target_values, 0)
        
        # 创建数据字典
        data_dict = {
            'X_test': [
                past_hour_features,
                cur_datetime_features,
                dayback_features
            ],
            'y_test': target_values
        }
        
        # 特征缩放 (使用已有的scaler进行transform)
        data_dict['X_test'][0] = self.feature_scaler.transform(
            data_dict['X_test'][0].reshape(-1, self.feature_size)
        ).reshape(data_dict['X_test'][0].shape)
        
        data_dict['X_test'][2] = self.dayback_scaler.transform(
            data_dict['X_test'][2]
        )
        
        data_dict['y_test'] = self.target_scaler.transform(
            data_dict['y_test'].reshape(-1, 1)
        )
        
        # 保存处理好的数据集
        self.save_test_data(data_dict)
        print("测试数据处理完成并已保存")

    def save_test_data(self, data_dict: dict, prefix: str = 'dataset'):
        """保存处理好的测试数据集"""
        try:
            # 保存测试集数据
            for i, feature_data in enumerate(data_dict['X_test']):
                filename = f"{prefix}_X_test_part{i}.npy"
                filepath = os.path.join(self.dataset_dir, filename)
                np.save(filepath, feature_data)
            
            filename = f"{prefix}_y_test.npy"
            filepath = os.path.join(self.dataset_dir, filename)
            np.save(filepath, data_dict['y_test'])
            
            # 保存缩放器
            scalers = {
                'feature_scaler': self.feature_scaler,
                'target_scaler': self.target_scaler,
                'dayback_scaler': self.dayback_scaler
            }
            scaler_path = os.path.join(self.dataset_dir, f"{prefix}_scalers.pkl")
            joblib.dump(scalers, scaler_path)
            
            print(f"测试数据集已保存到: {self.dataset_dir}")
            
        except Exception as e:
            raise RuntimeError(f"保存测试数据集时出错: {str(e)}")

def main():
    """主函数用于数据处理"""
    processor = DataProcessor()
    
    import argparse
    parser = argparse.ArgumentParser(description='数据处理脚本')
    parser.add_argument('--mode', type=str, default='train',
                      choices=['train', 'test'],
                      help='处理模式：train-训练集划分，test-整体作为测试集')
    args = parser.parse_args()
    
    if args.mode == 'train':
        processor.process_and_save_data()
    else:
        processor.process_and_save_as_test_data()

if __name__ == '__main__':
    main()