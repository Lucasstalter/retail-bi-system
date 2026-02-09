"""
Modelos de Machine Learning para Sistema BI Varejo
- Forecasting de Vendas (Prophet)
- Segmentação de Clientes (K-Means)
- Detecção de Anomalias
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    print("⚠️  Prophet não instalado. Instale com: pip install prophet")

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
import joblib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesForecastModel:
    """Modelo de Forecasting de Vendas usando Prophet"""
    
    def __init__(self, data_path='01_data/processed/fato_vendas.csv'):
        self.data_path = data_path
        self.model = None
        
    def prepare_data(self):
        """Prepara dados para o Prophet"""
        logger.info("Preparando dados para forecasting...")
        
        df = pd.read_csv(self.data_path, parse_dates=['Data_Venda'])
        
        # Agregar vendas diárias
        daily_sales = df.groupby('Data_Venda').agg({
            'Receita_Liquida': 'sum'
        }).reset_index()
        
        # Formato do Prophet: ds (data) e y (valor)
        daily_sales.columns = ['ds', 'y']
        
        self.train_data = daily_sales
        logger.info(f"✅ {len(daily_sales)} dias de dados preparados")
        
        return daily_sales
    
    def train(self, seasonality_mode='multiplicative'):
        """Treina o modelo Prophet"""
        if not PROPHET_AVAILABLE:
            logger.error("Prophet não está disponível")
            return False
        
        logger.info("Treinando modelo Prophet...")
        
        try:
            self.model = Prophet(
                seasonality_mode=seasonality_mode,
                daily_seasonality=False,
                weekly_seasonality=True,
                yearly_seasonality=True,
                changepoint_prior_scale=0.05
            )
            
            # Adicionar feriados brasileiros
            self.model.add_country_holidays(country_name='BR')
            
            # Treinar modelo
            self.model.fit(self.train_data)
            
            logger.info("✅ Modelo treinado com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro no treinamento: {str(e)}")
            return False
    
    def forecast(self, periods=90):
        """Gera previsão para os próximos N dias"""
        if self.model is None:
            logger.error("Modelo não foi treinado")
            return None
        
        logger.info(f"Gerando previsão para {periods} dias...")
        
        try:
            # Criar dataframe futuro
            future = self.model.make_future_dataframe(periods=periods)
            
            # Fazer previsão
            forecast = self.model.predict(future)
            
            # Selecionar apenas previsões futuras
            forecast_future = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periods)
            
            logger.info("✅ Previsão gerada com sucesso")
            
            return forecast_future
            
        except Exception as e:
            logger.error(f"❌ Erro na previsão: {str(e)}")
            return None
    
    def save_model(self, path='03_ml/models/sales_forecast_model.pkl'):
        """Salva o modelo treinado"""
        import os
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, 'wb') as f:
            joblib.dump(self.model, f)
        
        logger.info(f"✅ Modelo salvo em: {path}")
    
    def load_model(self, path='03_ml/models/sales_forecast_model.pkl'):
        """Carrega modelo salvo"""
        with open(path, 'rb') as f:
            self.model = joblib.load(f)
        
        logger.info(f"✅ Modelo carregado de: {path}")


class CustomerSegmentationModel:
    """Modelo de Segmentação de Clientes usando K-Means"""
    
    def __init__(self, rfm_path='01_data/processed/rfm_clientes.csv'):
        self.rfm_path = rfm_path
        self.model = None
        self.scaler = StandardScaler()
        
    def prepare_data(self):
        """Prepara dados RFM para clustering"""
        logger.info("Preparando dados para segmentação...")
        
        df = pd.read_csv(self.rfm_path)
        
        # Selecionar features numéricas
        features = ['Recencia', 'Frequencia', 'Monetario']
        self.X = df[features]
        self.cliente_ids = df['ID_Cliente']
        
        # Normalizar dados
        self.X_scaled = self.scaler.fit_transform(self.X)
        
        logger.info(f"✅ {len(df)} clientes preparados para segmentação")
        
        return self.X_scaled
    
    def find_optimal_clusters(self, max_k=10):
        """Encontra número ótimo de clusters usando método do cotovelo"""
        logger.info("Buscando número ótimo de clusters...")
        
        inertias = []
        K = range(2, max_k + 1)
        
        for k in K:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(self.X_scaled)
            inertias.append(kmeans.inertia_)
        
        # Retornar inércias para análise
        return K, inertias
    
    def train(self, n_clusters=5):
        """Treina modelo K-Means"""
        logger.info(f"Treinando K-Means com {n_clusters} clusters...")
        
        try:
            self.model = KMeans(
                n_clusters=n_clusters,
                random_state=42,
                n_init=10,
                max_iter=300
            )
            
            self.clusters = self.model.fit_predict(self.X_scaled)
            
            logger.info("✅ Modelo treinado com sucesso")
            
            # Estatísticas dos clusters
            unique, counts = np.unique(self.clusters, return_counts=True)
            logger.info("Distribuição de clusters:")
            for cluster, count in zip(unique, counts):
                logger.info(f"  Cluster {cluster}: {count} clientes ({count/len(self.clusters)*100:.1f}%)")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro no treinamento: {str(e)}")
            return False
    
    def get_cluster_profiles(self):
        """Analisa perfil de cada cluster"""
        if self.model is None:
            logger.error("Modelo não foi treinado")
            return None
        
        # Adicionar clusters aos dados originais
        df_clustered = self.X.copy()
        df_clustered['Cluster'] = self.clusters
        df_clustered['ID_Cliente'] = self.cliente_ids.values
        
        # Calcular médias por cluster
        profiles = df_clustered.groupby('Cluster').agg({
            'Recencia': 'mean',
            'Frequencia': 'mean',
            'Monetario': 'mean',
            'ID_Cliente': 'count'
        }).round(2)
        
        profiles.rename(columns={'ID_Cliente': 'Qtd_Clientes'}, inplace=True)
        
        return profiles
    
    def save_results(self, output_path='01_data/processed/clientes_segmentados.csv'):
        """Salva resultados da segmentação"""
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df_results = self.X.copy()
        df_results['Cluster'] = self.clusters
        df_results['ID_Cliente'] = self.cliente_ids.values
        
        df_results.to_csv(output_path, index=False)
        logger.info(f"✅ Resultados salvos em: {output_path}")
    
    def save_model(self, path='03_ml/models/customer_segmentation_model.pkl'):
        """Salva modelo e scaler"""
        import os
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler
        }
        
        with open(path, 'wb') as f:
            joblib.dump(model_data, f)
        
        logger.info(f"✅ Modelo salvo em: {path}")


class AnomalyDetectionModel:
    """Detecção de Anomalias em Vendas usando Isolation Forest"""
    
    def __init__(self, data_path='01_data/processed/fato_vendas.csv'):
        self.data_path = data_path
        self.model = None
        
    def prepare_data(self):
        """Prepara dados agregados para detecção de anomalias"""
        logger.info("Preparando dados para detecção de anomalias...")
        
        df = pd.read_csv(self.data_path, parse_dates=['Data_Venda'])
        
        # Agregar por dia
        daily = df.groupby('Data_Venda').agg({
            'Receita_Liquida': 'sum',
            'Quantidade': 'sum',
            'ID_Venda': 'count',
            'Desconto_Pct': 'mean'
        }).reset_index()
        
        daily.columns = ['Data', 'Receita', 'Quantidade', 'Num_Vendas', 'Desconto_Medio']
        
        # Features adicionais
        daily['Ticket_Medio'] = daily['Receita'] / daily['Num_Vendas']
        daily['Dia_Semana'] = daily['Data'].dt.dayofweek
        daily['Dia_Mes'] = daily['Data'].dt.day
        
        self.data = daily
        self.features = ['Receita', 'Quantidade', 'Num_Vendas', 'Ticket_Medio', 'Desconto_Medio']
        self.X = daily[self.features]
        
        logger.info(f"✅ {len(daily)} dias preparados")
        
        return self.X
    
    def train(self, contamination=0.05):
        """Treina Isolation Forest"""
        logger.info(f"Treinando Isolation Forest (contaminação={contamination})...")
        
        try:
            self.model = IsolationForest(
                contamination=contamination,
                random_state=42,
                n_estimators=100
            )
            
            self.predictions = self.model.fit_predict(self.X)
            
            # -1 para anomalias, 1 para normais
            n_anomalias = np.sum(self.predictions == -1)
            
            logger.info(f"✅ Modelo treinado - {n_anomalias} anomalias detectadas")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro no treinamento: {str(e)}")
            return False
    
    def get_anomalies(self):
        """Retorna dias com anomalias"""
        if self.model is None:
            logger.error("Modelo não foi treinado")
            return None
        
        anomalies = self.data.copy()
        anomalies['E_Anomalia'] = (self.predictions == -1)
        anomalies['Score_Anomalia'] = self.model.score_samples(self.X)
        
        # Retornar apenas anomalias ordenadas por score
        anomalies_only = anomalies[anomalies['E_Anomalia']].sort_values('Score_Anomalia')
        
        return anomalies_only
    
    def save_results(self, output_path='01_data/processed/anomalias_vendas.csv'):
        """Salva resultados da detecção"""
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        results = self.data.copy()
        results['E_Anomalia'] = (self.predictions == -1)
        results['Score_Anomalia'] = self.model.score_samples(self.X)
        
        results.to_csv(output_path, index=False)
        logger.info(f"✅ Resultados salvos em: {output_path}")


if __name__ == "__main__":
    print("="*60)
    print("🤖 MODELOS DE MACHINE LEARNING - BI VAREJO")
    print("="*60)
    
    # 1. Forecasting de Vendas
    if PROPHET_AVAILABLE:
        print("\n📈 1. FORECASTING DE VENDAS")
        print("-" * 60)
        forecast_model = SalesForecastModel()
        forecast_model.prepare_data()
        if forecast_model.train():
            forecast = forecast_model.forecast(periods=90)
            if forecast is not None:
                print("\nPrevisão para próximos 7 dias:")
                print(forecast.head(7))
                forecast_model.save_model()
    else:
        print("\n⚠️  Pulando Forecasting (Prophet não instalado)")
    
    # 2. Segmentação de Clientes
    print("\n\n👥 2. SEGMENTAÇÃO DE CLIENTES (K-MEANS)")
    print("-" * 60)
    seg_model = CustomerSegmentationModel()
    seg_model.prepare_data()
    
    if seg_model.train(n_clusters=5):
        profiles = seg_model.get_cluster_profiles()
        print("\nPerfil dos Clusters:")
        print(profiles)
        seg_model.save_results()
        seg_model.save_model()
    
    # 3. Detecção de Anomalias
    print("\n\n🔍 3. DETECÇÃO DE ANOMALIAS")
    print("-" * 60)
    anomaly_model = AnomalyDetectionModel()
    anomaly_model.prepare_data()
    
    if anomaly_model.train(contamination=0.05):
        anomalies = anomaly_model.get_anomalies()
        print(f"\nTop 5 dias com maior score de anomalia:")
        print(anomalies[['Data', 'Receita', 'Score_Anomalia']].head())
        anomaly_model.save_results()
    
    print("\n" + "="*60)
    print("✨ Todos os modelos foram treinados com sucesso!")
    print("="*60)
