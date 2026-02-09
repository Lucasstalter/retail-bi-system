#!/usr/bin/env python
"""
Script Principal - Sistema BI Varejo
Executa todo o pipeline: Geração de Dados -> ETL -> ML -> API
"""

import os
import sys
import subprocess
from datetime import datetime

def print_header(title):
    """Imprime header formatado"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")

def run_step(description, script_path, python_path="python"):
    """Executa um passo do pipeline"""
    print(f"🚀 {description}...")
    print(f"   Executando: {script_path}")
    print("-" * 70)
    
    try:
        result = subprocess.run(
            [python_path, script_path],
            check=True,
            capture_output=False,
            text=True
        )
        print(f"✅ {description} - CONCLUÍDO\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - ERRO")
        print(f"   Código de saída: {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ {description} - ERRO: {str(e)}")
        return False

def check_dependencies():
    """Verifica se as dependências estão instaladas"""
    print_header("VERIFICANDO DEPENDÊNCIAS")
    
    required_packages = [
        'pandas',
        'numpy',
        'faker',
        'scikit-learn',
        'fastapi',
        'uvicorn'
    ]
    
    missing = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - NÃO INSTALADO")
            missing.append(package)
    
    if missing:
        print(f"\n⚠️  Pacotes faltando: {', '.join(missing)}")
        print("   Execute: pip install -r requirements.txt")
        return False
    
    print("\n✅ Todas as dependências estão instaladas!")
    return True

def main():
    """Função principal"""
    start_time = datetime.now()
    
    print_header("SISTEMA DE BI COMPLETO PARA VAREJO")
    print("Autor: Lucas")
    print("Versão: 1.0.0")
    print(f"Início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Verificar dependências
    if not check_dependencies():
        print("\n❌ Pipeline interrompido - instale as dependências primeiro")
        sys.exit(1)
    
    # Pipeline de execução
    steps = [
        {
            'description': 'Passo 1: Geração de Dados Sintéticos',
            'script': '01_data/synthetic_generator.py',
            'required': True
        },
        {
            'description': 'Passo 2: Pipeline ETL',
            'script': '02_etl/pipeline.py',
            'required': True
        },
        {
            'description': 'Passo 3: Treinamento de Modelos ML',
            'script': '03_ml/models_training.py',
            'required': False  # Opcional se Prophet não estiver instalado
        }
    ]
    
    # Executar passos
    success_count = 0
    
    for i, step in enumerate(steps, 1):
        print_header(f"{step['description']}")
        
        if run_step(step['description'], step['script']):
            success_count += 1
        elif step['required']:
            print("\n❌ Pipeline interrompido devido a erro crítico")
            sys.exit(1)
        else:
            print("\n⚠️  Passo opcional falhou - continuando...")
    
    # Resumo final
    end_time = datetime.now()
    duration = end_time - start_time
    
    print_header("RESUMO DA EXECUÇÃO")
    print(f"✅ Passos concluídos: {success_count}/{len(steps)}")
    print(f"⏱️  Tempo total: {duration.total_seconds():.2f} segundos")
    print(f"📁 Dados gerados em: 01_data/processed/")
    print(f"🤖 Modelos salvos em: 03_ml/models/")
    
    print("\n" + "="*70)
    print("PRÓXIMOS PASSOS:")
    print("="*70)
    print("1. Abrir Power BI Desktop")
    print("   - Arquivo: 04_powerbi/dashboard.pbix")
    print("   - Atualizar fontes de dados")
    print("")
    print("2. Iniciar API REST")
    print("   - cd 05_api")
    print("   - python main.py")
    print("   - Acessar: http://localhost:8000/docs")
    print("")
    print("3. Ver documentação completa")
    print("   - README.md")
    print("   - 06_docs/ARCHITECTURE.md")
    print("="*70)
    
    print("\n✨ Pipeline executado com sucesso!")
    print("🎉 Projeto pronto para portfolio!\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Erro inesperado: {str(e)}")
        sys.exit(1)
