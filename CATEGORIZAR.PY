import pandas as pd
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

# Caminho do arquivo original
file_path = 'D:/01-Estudos/01-Power BI/03-Relatórios/01-Fluxo Caixa/BaseDados/Itau/arq_importados/2024-11.TXT'

# Nome do arquivo de saída (mesmo nome do original com "v2" adicionado antes da extensão)
nome_arquivo = os.path.basename(file_path)
nome_arquivo_v2 = nome_arquivo.replace('.txt', '_v2.txt')

# Defina o diretório de destino (aqui você pode especificar o caminho da pasta)
output_dir = 'D:/01-Estudos/01-Power BI/03-Relatórios/01-Fluxo Caixa/BaseDados/Itau/'

# Caminho completo para salvar o arquivo com "v2"
output_path = os.path.join(output_dir, nome_arquivo_v2)

# Ler o arquivo TXT, considerando o delimitador ';'
df = pd.read_csv(file_path, delimiter=';', header=None)

# Adicionar a linha de cabeçalho
df.columns = ['data_lanc', 'descricao', 'valor']

# Adicionar a coluna 'nome_arquivo'
df['nome_arquivo'] = nome_arquivo

# Incluir as colunas restantes como placeholders
df['id_fluxo'] = ''
df['id_forma_pgto'] = ''
df['observacao'] = ''

# Vetorizar as descrições para entender o conteúdo textual
vectorizer = TfidfVectorizer(stop_words='english', max_features=10)  # 'max_features' limita o número de termos analisados
X = vectorizer.fit_transform(df['descricao'])

# Usando KMeans para agrupar as descrições
num_clusters = 5  # Número de clusters que você deseja
kmeans = KMeans(n_clusters=num_clusters, random_state=42)
df['id_categoria'] = kmeans.fit_predict(X)

# Reorganizar as colunas na ordem desejada
df = df[['data_lanc', 'descricao', 'valor', 'id_fluxo', 'id_categoria', 'id_forma_pgto', 'observacao', 'nome_arquivo']]

# Salvar o arquivo com o cabeçalho adicionado
df.to_csv(output_path, index=False, sep=';')

# Mostrar o resultado (opcional)
print(f"Arquivo salvo em: {output_path}")
