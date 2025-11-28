#Bibliotecas
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import math
from matplotlib.gridspec import GridSpec
from pathlib import Path
from sqlalchemy import text

def gerar_grafico_sql(codigo_sql, coluna_x, coluna_y, rotulo_valor, titulo_grafico, tipo_grafico, nome_arquivo_saida, conexao):
    """
    Executa SQL, processa datas e gera um gráfico (Linha ou Barra) com RÓTULOS DE DADOS, salvando em PNG.
    """
    
    try:
        # 1. Executa a consulta SQL
        df = pd.read_sql(sql=text(codigo_sql), con=conexao)
        
        # 2. PRÉ-PROCESSAMENTO (Lógica de Data)
        if coluna_x == 'periodo' and 'periodo' not in df.columns:
            if 'year' in df.columns and 'month' in df.columns:
                df['periodo'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
            else:
                print("Aviso: Eixo X definido como 'periodo', mas colunas 'year'/'month' não encontradas.")

        # Dicionário para renomear os labels visualmente
        labels_map = {
            coluna_x: coluna_x.capitalize(), 
            coluna_y: rotulo_valor
        }

        # 3. PLOTAGEM
        if tipo_grafico.lower() in ['linhas', 'linha', 'line']:
            # Adicionado parâmetro text=coluna_y
            fig = px.line(df, x=coluna_x, y=coluna_y, title=titulo_grafico, labels=labels_map)
            
        elif tipo_grafico.lower() in ['barras', 'barra', 'bar']:
            # Adicionado parâmetro text=coluna_y
            fig = px.bar(df, x=coluna_x, y=coluna_y, title=titulo_grafico, labels=labels_map, text=coluna_y)
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            
        else:
            print(f"Erro: Tipo de gráfico '{tipo_grafico}' não reconhecido.")
            return
        
        # Ajuste específico para datas no eixo X
        if coluna_x == 'periodo':
            fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")

        # 4. EXIBE E SALVA
        fig.show() # Descomente se quiser ver no notebook
        
        try:
            fig.write_image(nome_arquivo_saida)
            print(f"Sucesso: Gráfico salvo em '{nome_arquivo_saida}'")
        except Exception as e:
            print(f"Erro ao salvar imagem PNG (Verifique o pacote 'kaleido'): {e}")
            
    except Exception as e:
        print(f"Erro geral na execução da função: {e}")

# --- EXEMPLO DE USO ---

# consulta_sql = """
# select dd.year, dd.month, sum(fs.price) as sales
# from fact_sales fs
# join dim_date dd on fs.date_sk = dd.date_sk
# group by dd.year, dd.month
# order by dd.year, dd.month;
# """

# gerar_grafico_sql(
#     codigo_sql=consulta_sql,
#     coluna_x='periodo',
#     coluna_y='sales',
#     rotulo_valor='Total de Vendas (R$)',
#     titulo_grafico='Evolução de Vendas Mensais',
#     tipo_grafico='linhas',
#     nome_arquivo_saida='visualizacoes/grafico_dinamico.png',
#     conexao=cn.get_engine()
# )

def plotar_comparacao_produtos(df, arquivo_saida='grafico_comparativo.png'):
    """
    Recebe um DataFrame com as colunas ['produto', 'melhor_mes', 'qtd_vendas_max', 
    'pior_mes', 'qtd_vendas_min'] e gera uma grade de gráficos comparativos.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados dos Top Produtos.
        arquivo_saida (str): Nome do arquivo de imagem a ser salvo.
    """
    
    # 1. Definição dinâmica do tamanho da grade (Grid)
    qtd_itens = len(df)
    cols = 2  # Fixamos em 2 colunas para boa leitura
    rows = math.ceil(qtd_itens / cols) # Calcula quantas linhas são necessárias

    # Ajusta o tamanho da figura baseado no número de linhas (4 unidades de altura por linha)
    fig_height = rows * 4
    fig, axes = plt.subplots(nrows=rows, ncols=cols, figsize=(15, fig_height))
    
    # "Achata" a matriz de eixos para facilitar o loop (funciona mesmo se tiver apenas 1 linha)
    axes = axes.flatten() 

    # 2. Geração dos Subplots
    for i, row in df.iterrows():
        # Verifica se o índice ainda está dentro do número de eixos disponíveis
        if i < len(axes):
            ax = axes[i]
            
            # Prepara os dados
            meses = [f"Melhor:\n{row['melhor_mes']}", f"Pior:\n{row['pior_mes']}"]
            vendas = [row['qtd_vendas_max'], row['qtd_vendas_min']]
            #cores = ['#55A868', '#C44E52'] # Verde (Melhor) e Vermelho (Pior)
            #cores = ['#8FBC8F', '#FA8072']
            cores = ['#66C2A5', '#FC8D62']
            
            # Cria as barras
            barras = ax.bar(meses, vendas, color=cores)
            
            # Estilização
            ax.set_title(f"Produto {row['produto']}", fontsize=12, fontweight='bold')
            ax.set_ylabel('Qtd Vendas')
            
            # Adiciona rótulos de dados (Data Labels)
            for barra in barras:
                altura = barra.get_height()
                ax.annotate(f'{altura}',
                            xy=(barra.get_x() + barra.get_width() / 2, altura),
                            xytext=(0, 3), 
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=10, fontweight='bold')

            # Margem superior no eixo Y
            ax.set_ylim(0, max(vendas) * 1.2)
    
    # 3. Limpeza de eixos vazios (caso o número de itens seja ímpar)
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    # 4. Ajustes Finais e Salvamento
    plt.tight_layout(pad=3.0)
    plt.savefig(arquivo_saida)
    plt.show() # Opcional: comentar se for rodar em script sem interface
    print(f"Gráfico salvo com sucesso em: {arquivo_saida}")





def gerar_dashboard_consolidado(pasta_origem='visualizations', nome_arquivo_saida='dashboard_final.png'):
    """
    Lê imagens PNG de gráficos gerados anteriormente e as consolida em um único 
    Layout de Dashboard (3 linhas x 2 colunas).

    Args:
        pasta_origem (str): Nome da pasta onde estão os PNGs (ex: 'visualizations').
        nome_arquivo_saida (str): Nome do arquivo final a ser salvo.
    """
    
    # Define o caminho base usando Pathlib para compatibilidade de SO
    base_dir = Path(pasta_origem)
    output_path = base_dir / nome_arquivo_saida

    # Dicionário mapeando a posição lógica para o nome do arquivo esperado
    # Certifique-se que estes nomes batem com o que foi gerado nos passos anteriores
    imagens_map = {
        "evolucao": "grafico_vendas_mensais.png",
        "top_itens": "grafico_top10_itens_mais_vendidos.png",
        "top_clientes": "grafico_top10_clientes.png",
        "tempo_entrega": "grafico_tempo_medio_atendimento.png",
        "comparativo": "grafico_melhor-e-pior-desempenho_itens.png"
    }

    # Configuração da Figura (Canvas)
    # Height ratios: A última linha é um pouco maior (1.5) para acomodar o gráfico comparativo
    fig = plt.figure(figsize=(24, 20))
    gs = GridSpec(3, 2, figure=fig, height_ratios=[1, 1, 1.5]) 

    # Título Geral do Dashboard
    plt.suptitle("Dashboard Analítico de E-commerce (Olist)", fontsize=26, fontweight='bold', y=0.95)

    # --- Função auxiliar para plotar imagens ---
    def plotar_imagem(ax_position, key_name, titulo_custom=None):
        arquivo = base_dir / imagens_map[key_name]
        if arquivo.exists():
            img = mpimg.imread(str(arquivo))
            ax = fig.add_subplot(ax_position)
            ax.imshow(img)
            ax.axis('off') # Remove bordas e eixos da imagem
            if titulo_custom:
                ax.set_title(titulo_custom, fontsize=16, fontweight='bold')
        else:
            print(f"⚠️ Aviso: Imagem não encontrada para compor o dashboard: {arquivo}")
            # Cria um placeholder vazio para não quebrar o layout
            ax = fig.add_subplot(ax_position)
            ax.text(0.5, 0.5, 'Imagem não disponível', ha='center', va='center')
            ax.axis('off')

    # --- Montagem do Grid ---

    # 1. LINHA SUPERIOR (Inteira): Evolução Temporal
    plotar_imagem(gs[0, :], "evolucao", "Visão Geral: Evolução de Vendas")

    # 2. LINHA DO MEIO (Esquerda): Top Itens
    plotar_imagem(gs[1, 0], "top_itens")

    # 3. LINHA DO MEIO (Direita): Top Clientes
    plotar_imagem(gs[1, 1], "top_clientes")

    # 4. LINHA INFERIOR (Esquerda): Tempo de Entrega
    plotar_imagem(gs[2, 0], "tempo_entrega")

    # 5. LINHA INFERIOR (Direita): Comparativo Melhor/Pior
    plotar_imagem(gs[2, 1], "comparativo")

    # Finalização
    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Ajusta margens para o suptitle não cortar
    
    # Salva o arquivo final
    try:
        plt.savefig(output_path, dpi=100, bbox_inches='tight')
        print(f"✅ Dashboard consolidado salvo com sucesso em: {output_path}")
    except Exception as e:
        print(f"❌ Erro ao salvar o dashboard: {e}")

    # Exibe no notebook
    plt.show()