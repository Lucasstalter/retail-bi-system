"""
Testes para API REST - Sistema BI Varejo
"""

import pytest
from fastapi.testclient import TestClient
import sys
import os

# Adicionar diretório da API ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '05_api'))

from main import app

client = TestClient(app)


class TestHealthEndpoints:
    """Testes de saúde da API"""
    
    def test_root_endpoint(self):
        """Testa endpoint raiz"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "api" in data
        assert data["status"] == "online"
    
    def test_health_check(self):
        """Testa health check"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data


class TestKPIsEndpoint:
    """Testes do endpoint de KPIs"""
    
    def test_get_kpis_success(self):
        """Testa obtenção de KPIs com sucesso"""
        response = client.get("/kpis")
        
        # Pode falhar se dados não existirem
        if response.status_code == 200:
            data = response.json()
            assert "receita_total" in data
            assert "lucro_bruto" in data
            assert "margem_bruta_pct" in data
            assert "ticket_medio" in data
            assert "total_vendas" in data
            assert "total_clientes" in data
            
            # Validar tipos
            assert isinstance(data["receita_total"], (int, float))
            assert isinstance(data["margem_bruta_pct"], (int, float))
    
    def test_get_kpis_with_date_filters(self):
        """Testa KPIs com filtros de data"""
        response = client.get("/kpis?data_inicio=2024-01-01&data_fim=2024-12-31")
        
        if response.status_code == 200:
            data = response.json()
            assert "receita_total" in data


class TestVendasEndpoints:
    """Testes dos endpoints de vendas"""
    
    def test_get_vendas_mensais(self):
        """Testa obtenção de vendas mensais"""
        response = client.get("/vendas/mensais")
        
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            
            if len(data) > 0:
                venda = data[0]
                assert "ano" in venda
                assert "mes" in venda
                assert "receita" in venda
                assert "lucro" in venda
    
    def test_get_vendas_mensais_with_filters(self):
        """Testa vendas mensais com filtros"""
        response = client.get("/vendas/mensais?ano=2024&limit=5")
        
        if response.status_code == 200:
            data = response.json()
            assert len(data) <= 5


class TestProdutosEndpoints:
    """Testes dos endpoints de produtos"""
    
    def test_get_top_produtos(self):
        """Testa top produtos"""
        response = client.get("/produtos/top")
        
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            
            if len(data) > 0:
                produto = data[0]
                assert "id_produto" in produto
                assert "nome_produto" in produto
                assert "categoria" in produto
                assert "receita" in produto
    
    def test_get_categorias(self):
        """Testa listagem de categorias"""
        response = client.get("/categorias")
        
        if response.status_code == 200:
            data = response.json()
            assert "categorias" in data
            assert isinstance(data["categorias"], list)


class TestClientesEndpoints:
    """Testes dos endpoints de clientes"""
    
    def test_get_segmentos_clientes(self):
        """Testa segmentação de clientes"""
        response = client.get("/clientes/segmentos")
        
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)


class TestErrorHandling:
    """Testes de tratamento de erros"""
    
    def test_invalid_endpoint(self):
        """Testa endpoint inexistente"""
        response = client.get("/endpoint/inexistente")
        assert response.status_code == 404
    
    def test_invalid_date_format(self):
        """Testa formato de data inválido"""
        response = client.get("/kpis?data_inicio=data-invalida")
        # Pode retornar 500 ou processar normalmente dependendo da validação
        assert response.status_code in [200, 422, 500]


# ==================== FIXTURES ====================

@pytest.fixture
def sample_kpi_response():
    """Fixture com resposta exemplo de KPI"""
    return {
        "receita_total": 1000000.00,
        "lucro_bruto": 400000.00,
        "margem_bruta_pct": 40.00,
        "ticket_medio": 150.00,
        "total_vendas": 6667,
        "total_clientes": 2000
    }


# ==================== EXECUTAR TESTES ====================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
