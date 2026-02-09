"""
Pipeline ETL para Sistema de BI de Varejo
Extração, Transformação e Carga de dados
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Pipeline completo de ETL para dados de varejo"""
    
    def __init__(self, input_dir='01_data/processed', output_dir='01_data/processed'):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
    def extract(self):
        """Extrai dados dos CSVs"""
        logger.info("Iniciando extração de dados...")
        
        try:
            self.dim_tempo = pd.read_csv(f'{self.input_dir}/dim_tempo.csv', parse_dates=['Data'])
            self.dim_produto = pd.read_csv(f'{self.input_dir}/dim_produto.csv')
            self.dim_loja = pd.read_csv(f'{self.input_dir}/dim_loja.csv', parse_dates=['Data_Abertura'])
            self.dim_cliente = pd.read_csv(f'{self.input_dir}/dim_cliente.csv', parse_dates=['Data_Nascimento', 'Data_Cadastro'])
            self.dim_vendedor = pd.read_csv(f'{self.input_dir}/dim_vendedor.csv', parse_dates=['Data_Admissao'])
            self.fato_vendas = pd.read_csv(f'{self.input_dir}/fato_vendas.csv', parse_dates=['Data_Venda'])
            
            logger.info("✅ Dados extraídos com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro na extração: {str(e)}")
            return False
    
    def transform(self):
        """Aplica transformações e cria métricas derivadas"""
        logger.info("Iniciando transformações...")
        
        try:
            # Transformações em Fato Vendas
            self.fato_vendas['Ano'] = self.fato_vendas['Data_Venda'].dt.year
            self.fato_vendas['Mes'] = self.fato_vendas['Data_Venda'].dt.month
            self.fato_vendas['Dia_Semana'] = self.fato_vendas['Data_Venda'].dt.dayofweek
            self.fato_vendas['Margem_Pct'] = (
                (self.fato_vendas['Lucro_Bruto'] / self.fato_vendas['Receita_Liquida']) * 100
            ).round(2)
            
            # Criar tabela agregada mensal
            self.vendas_mensais = self.fato_vendas.groupby(['Ano', 'Mes']).agg({
                'Receita_Liquida': 'sum',
                'Lucro_Bruto': 'sum',
                'Quantidade': 'sum',
                'ID_Venda': 'count'
            }).reset_index()
            
            self.vendas_mensais.rename(columns={'ID_Venda': 'Qtd_Vendas'}, inplace=True)
            self.vendas_mensais['Ticket_Medio'] = (
                self.vendas_mensais['Receita_Liquida'] / self.vendas_mensais['Qtd_Vendas']
            ).round(2)
            
            # Criar análise de clientes (RFM)
            hoje = pd.Timestamp.now()
            
            rfm = self.fato_vendas.groupby('ID_Cliente').agg({
                'Data_Venda': lambda x: (hoje - x.max()).days,  # Recência
                'ID_Venda': 'count',  # Frequência
                'Receita_Liquida': 'sum'  # Monetário
            }).reset_index()
            
            rfm.columns = ['ID_Cliente', 'Recencia', 'Frequencia', 'Monetario']
            
            # Criar scores RFM (1-5)
            rfm['R_Score'] = pd.qcut(rfm['Recencia'], 5, labels=[5,4,3,2,1], duplicates='drop')
            rfm['F_Score'] = pd.qcut(rfm['Frequencia'], 5, labels=[1,2,3,4,5], duplicates='drop')
            rfm['M_Score'] = pd.qcut(rfm['Monetario'], 5, labels=[1,2,3,4,5], duplicates='drop')
            
            rfm['RFM_Score'] = (
                rfm['R_Score'].astype(str) + 
                rfm['F_Score'].astype(str) + 
                rfm['M_Score'].astype(str)
            )
            
            # Segmentar clientes
            def segment_customer(score):
                if score[0] in ['4', '5'] and score[1] in ['4', '5']:
                    return 'Champions'
                elif score[0] in ['3', '4', '5'] and score[1] in ['3', '4']:
                    return 'Leais'
                elif score[0] in ['4', '5'] and score[1] in ['1', '2']:
                    return 'Potencial'
                elif score[0] in ['2', '3'] and score[1] in ['2', '3', '4']:
                    return 'Em Risco'
                else:
                    return 'Perdidos'
            
            rfm['Segmento_RFM'] = rfm['RFM_Score'].apply(segment_customer)
            self.rfm_clientes = rfm
            
            # Criar análise ABC de produtos
            vendas_produto = self.fato_vendas.groupby('ID_Produto').agg({
                'Receita_Liquida': 'sum',
                'Quantidade': 'sum'
            }).reset_index()
            
            vendas_produto = vendas_produto.sort_values('Receita_Liquida', ascending=False)
            vendas_produto['Receita_Acumulada'] = vendas_produto['Receita_Liquida'].cumsum()
            total_receita = vendas_produto['Receita_Liquida'].sum()
            vendas_produto['Pct_Acumulado'] = (vendas_produto['Receita_Acumulada'] / total_receita * 100).round(2)
            
            def classificar_abc(pct):
                if pct <= 80:
                    return 'A'
                elif pct <= 95:
                    return 'B'
                else:
                    return 'C'
            
            vendas_produto['Classe_ABC'] = vendas_produto['Pct_Acumulado'].apply(classificar_abc)
            self.abc_produtos = vendas_produto
            
            logger.info("✅ Transformações aplicadas com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro nas transformações: {str(e)}")
            return False
    
    def load(self):
        """Carrega dados transformados"""
        logger.info("Iniciando carga de dados transformados...")
        
        try:
            # Salvar tabelas agregadas
            self.vendas_mensais.to_csv(f'{self.output_dir}/vendas_mensais.csv', index=False)
            self.rfm_clientes.to_csv(f'{self.output_dir}/rfm_clientes.csv', index=False)
            self.abc_produtos.to_csv(f'{self.output_dir}/abc_produtos.csv', index=False)
            
            # Atualizar fato vendas com campos derivados
            self.fato_vendas.to_csv(f'{self.output_dir}/fato_vendas.csv', index=False)
            
            logger.info("✅ Dados carregados com sucesso")
            
            # Estatísticas
            print("\n" + "="*60)
            print("📊 ESTATÍSTICAS DO PIPELINE ETL")
            print("="*60)
            print(f"Vendas Mensais: {len(self.vendas_mensais)} registros")
            print(f"Clientes RFM: {len(self.rfm_clientes)} registros")
            print(f"Produtos ABC: {len(self.abc_produtos)} registros")
            print("\nDistribuição ABC de Produtos:")
            print(self.abc_produtos['Classe_ABC'].value_counts())
            print("\nSegmentação RFM de Clientes:")
            print(self.rfm_clientes['Segmento_RFM'].value_counts())
            print("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro na carga: {str(e)}")
            return False
    
    def run(self):
        """Executa pipeline completo"""
        logger.info("🚀 Iniciando Pipeline ETL")
        
        if not self.extract():
            return False
        
        if not self.transform():
            return False
        
        if not self.load():
            return False
        
        logger.info("✨ Pipeline ETL concluído com sucesso!")
        return True


if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()
