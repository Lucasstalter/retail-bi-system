"""
API FastAPI para Sistema BI Varejo
Endpoints para KPIs, dados e previsões
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import os

app = FastAPI(
    title="API BI Varejo",
    description="API REST para Sistema de Business Intelligence de Varejo",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Diretório de dados
DATA_DIR = "01_data/processed"

# ==================== MODELOS PYDANTIC ====================

class KPIResponse(BaseModel):
    receita_total: float
    lucro_bruto: float
    margem_bruta_pct: float
    ticket_medio: float
    total_vendas: int
    total_clientes: int

class VendaMensal(BaseModel):
    ano: int
    mes: int
    receita: float
    lucro: float
    qtd_vendas: int
    ticket_medio: float

class ProdutoTop(BaseModel):
    id_produto: int
    nome_produto: str
    categoria: str
    receita: float
    quantidade: int

class ClienteSegmento(BaseModel):
    segmento: str
    qtd_clientes: int
    receita_media: float
    frequencia_media: float

# ==================== FUNÇÕES AUXILIARES ====================

def carregar_dados():
    """Carrega dados dos CSVs"""
    try:
        fato_vendas = pd.read_csv(f'{DATA_DIR}/fato_vendas.csv', parse_dates=['Data_Venda'])
        dim_produto = pd.read_csv(f'{DATA_DIR}/dim_produto.csv')
        rfm_clientes = pd.read_csv(f'{DATA_DIR}/rfm_clientes.csv')
        vendas_mensais = pd.read_csv(f'{DATA_DIR}/vendas_mensais.csv')
        
        return fato_vendas, dim_produto, rfm_clientes, vendas_mensais
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar dados: {str(e)}")

# ==================== ENDPOINTS ====================

@app.get("/")
async def root():
    """Endpoint raiz com informações da API"""
    return {
        "api": "BI Varejo API",
        "version": "1.0.0",
        "status": "online",
        "endpoints": {
            "/kpis": "KPIs gerais do negócio",
            "/vendas/mensais": "Vendas agregadas por mês",
            "/produtos/top": "Top produtos por receita",
            "/clientes/segmentos": "Segmentação de clientes",
            "/forecast": "Previsão de vendas",
            "/health": "Status da API"
        }
    }

@app.get("/health")
async def health_check():
    """Verifica saúde da API"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/kpis", response_model=KPIResponse)
async def get_kpis(
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None
):
    """
    Retorna KPIs principais do negócio
    
    Parâmetros:
    - data_inicio: Data inicial (formato YYYY-MM-DD)
    - data_fim: Data final (formato YYYY-MM-DD)
    """
    try:
        fato_vendas, _, _, _ = carregar_dados()
        
        # Filtrar por data se fornecido
        if data_inicio:
            fato_vendas = fato_vendas[fato_vendas['Data_Venda'] >= data_inicio]
        if data_fim:
            fato_vendas = fato_vendas[fato_vendas['Data_Venda'] <= data_fim]
        
        # Calcular KPIs
        receita_total = float(fato_vendas['Receita_Liquida'].sum())
        lucro_bruto = float(fato_vendas['Lucro_Bruto'].sum())
        margem_bruta_pct = (lucro_bruto / receita_total * 100) if receita_total > 0 else 0
        ticket_medio = float(fato_vendas['Receita_Liquida'].mean())
        total_vendas = int(len(fato_vendas))
        total_clientes = int(fato_vendas['ID_Cliente'].nunique())
        
        return KPIResponse(
            receita_total=round(receita_total, 2),
            lucro_bruto=round(lucro_bruto, 2),
            margem_bruta_pct=round(margem_bruta_pct, 2),
            ticket_medio=round(ticket_medio, 2),
            total_vendas=total_vendas,
            total_clientes=total_clientes
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vendas/mensais", response_model=List[VendaMensal])
async def get_vendas_mensais(
    ano: Optional[int] = None,
    limit: int = Query(default=12, le=100)
):
    """
    Retorna vendas agregadas por mês
    
    Parâmetros:
    - ano: Filtrar por ano específico
    - limit: Número máximo de meses a retornar
    """
    try:
        _, _, _, vendas_mensais = carregar_dados()
        
        if ano:
            vendas_mensais = vendas_mensais[vendas_mensais['Ano'] == ano]
        
        vendas_mensais = vendas_mensais.sort_values(['Ano', 'Mes'], ascending=False).head(limit)
        
        resultado = []
        for _, row in vendas_mensais.iterrows():
            resultado.append(VendaMensal(
                ano=int(row['Ano']),
                mes=int(row['Mes']),
                receita=float(row['Receita_Liquida']),
                lucro=float(row['Lucro_Bruto']),
                qtd_vendas=int(row['Qtd_Vendas']),
                ticket_medio=float(row['Ticket_Medio'])
            ))
        
        return resultado
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/produtos/top", response_model=List[ProdutoTop])
async def get_top_produtos(
    limit: int = Query(default=10, le=100),
    categoria: Optional[str] = None
):
    """
    Retorna top produtos por receita
    
    Parâmetros:
    - limit: Quantidade de produtos a retornar
    - categoria: Filtrar por categoria específica
    """
    try:
        fato_vendas, dim_produto, _, _ = carregar_dados()
        
        # Agregar vendas por produto
        vendas_produto = fato_vendas.groupby('ID_Produto').agg({
            'Receita_Liquida': 'sum',
            'Quantidade': 'sum'
        }).reset_index()
        
        # Merge com dimensão produto
        vendas_produto = vendas_produto.merge(dim_produto, on='ID_Produto')
        
        # Filtrar por categoria se fornecido
        if categoria:
            vendas_produto = vendas_produto[vendas_produto['Categoria'] == categoria]
        
        # Ordenar e limitar
        vendas_produto = vendas_produto.sort_values('Receita_Liquida', ascending=False).head(limit)
        
        resultado = []
        for _, row in vendas_produto.iterrows():
            resultado.append(ProdutoTop(
                id_produto=int(row['ID_Produto']),
                nome_produto=str(row['Nome_Produto']),
                categoria=str(row['Categoria']),
                receita=float(row['Receita_Liquida']),
                quantidade=int(row['Quantidade'])
            ))
        
        return resultado
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/clientes/segmentos", response_model=List[ClienteSegmento])
async def get_segmentos_clientes():
    """Retorna análise de segmentação de clientes (RFM)"""
    try:
        fato_vendas, _, rfm_clientes, _ = carregar_dados()
        
        # Calcular estatísticas por segmento
        vendas_cliente = fato_vendas.groupby('ID_Cliente').agg({
            'Receita_Liquida': 'sum',
            'ID_Venda': 'count'
        }).reset_index()
        
        vendas_cliente.columns = ['ID_Cliente', 'Receita_Total', 'Frequencia']
        
        # Merge com RFM
        if 'Segmento_RFM' in rfm_clientes.columns:
            rfm_vendas = rfm_clientes.merge(vendas_cliente, on='ID_Cliente')
            
            segmentos = rfm_vendas.groupby('Segmento_RFM').agg({
                'ID_Cliente': 'count',
                'Receita_Total': 'mean',
                'Frequencia': 'mean'
            }).reset_index()
            
            segmentos.columns = ['Segmento', 'Qtd_Clientes', 'Receita_Media', 'Frequencia_Media']
            
            resultado = []
            for _, row in segmentos.iterrows():
                resultado.append(ClienteSegmento(
                    segmento=str(row['Segmento']),
                    qtd_clientes=int(row['Qtd_Clientes']),
                    receita_media=float(row['Receita_Media']),
                    frequencia_media=float(row['Frequencia_Media'])
                ))
            
            return resultado
        else:
            # Retornar vazio se não houver segmentação
            return []
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vendas/diarias")
async def get_vendas_diarias(
    data_inicio: str,
    data_fim: str
):
    """
    Retorna vendas agregadas por dia
    
    Parâmetros:
    - data_inicio: Data inicial (YYYY-MM-DD)
    - data_fim: Data final (YYYY-MM-DD)
    """
    try:
        fato_vendas, _, _, _ = carregar_dados()
        
        # Filtrar por período
        mask = (fato_vendas['Data_Venda'] >= data_inicio) & (fato_vendas['Data_Venda'] <= data_fim)
        vendas_periodo = fato_vendas[mask]
        
        # Agregar por dia
        vendas_diarias = vendas_periodo.groupby('Data_Venda').agg({
            'Receita_Liquida': 'sum',
            'Lucro_Bruto': 'sum',
            'ID_Venda': 'count'
        }).reset_index()
        
        vendas_diarias.columns = ['Data', 'Receita', 'Lucro', 'Qtd_Vendas']
        
        # Converter para JSON serializável
        resultado = vendas_diarias.to_dict(orient='records')
        
        # Converter datas para string
        for item in resultado:
            item['Data'] = str(item['Data'])[:10]
        
        return JSONResponse(content=resultado)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/categorias")
async def get_categorias():
    """Retorna lista de categorias de produtos"""
    try:
        _, dim_produto, _, _ = carregar_dados()
        
        categorias = dim_produto['Categoria'].unique().tolist()
        
        return {"categorias": categorias}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== EXECUTAR ====================

if __name__ == "__main__":
    import uvicorn
    
    print("="*60)
    print("🚀 Iniciando API BI Varejo")
    print("="*60)
    print("📍 URL: http://localhost:8000")
    print("📚 Docs: http://localhost:8000/docs")
    print("="*60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
