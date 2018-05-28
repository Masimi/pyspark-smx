
# coding: utf-8

# ### Carregamento das Lib's utilizadas

# In[1]:


import os
from pyspark import SparkContext, SparkConf, SQLContext


# Preparando contexto

# In[2]:


sc.stop()
sc = SparkContext().getOrCreate()


# In[3]:


sqlContext = SQLContext(sc)


# Carregando Arquivos

# In[4]:


textFile = sc.textFile(r"C:\projects\python-first-steps-spark\data\access_log_Jul95,C:\projects\python-first-steps-spark\data\access_log_Aug95")


# ## 1. Número de hosts únicos.

# In[5]:


#counts = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).collect()


# In[6]:


# Separa os dados de host
hosts = textFile.map(lambda line: line.split()[0])


# In[7]:


# Realiza um group by nos hosts
uniqueHosts = hosts.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)


# In[8]:


# Realiza a contagem de hosts unicos
uniqueHosts.count()


# resultado 137.979 hosts unicos.

# ## 2. O total de erros 404.

# In[9]:


#Separa os dados de requisicoesque ocorreram 404
erro_404 = textFile.filter(lambda x: "404 -" in x)


# In[10]:


#Realiza a contagem nas requisiçoes 404
erro_404.count()


# Resultado 20.900 erros do tipo 404.

# ### 3. Os 5 URLs que mais causaram erro 404.

# In[11]:


#Separa as url's das requisicoes que ocorreram 404
url_404 = erro_404.map(lambda line: line.split()[0])


# In[12]:


#Agrupamento e contagem das url's
count_url = url_404.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)


# In[13]:


#Orderna pela quantidade de forma decrescente e plota as 5 primeiras url's
count_url.sortBy(lambda x: x[1],False).take(5)


# ### 4. Quantidade de erros 404 por dia.

# In[14]:


# Separa os dados de datas das requisicoes que ocorreram 404
time = erro_404.map(lambda line: line.split(" ")[3:5])


# In[15]:


# Cria-se um dataFrame
t = sqlContext.createDataFrame(time,['date'])


# In[16]:


# Extraindo somente a data (nao incluso hora, minuto etc)
dates = t.select(t.date.substr(2, 12).alias("date"))


# In[17]:


#Soma a qtde de ocorrencias(404) e orderna de forma decrescente
erro_day = dates.groupBy("date").count().orderBy('count', ascending=False)


# In[18]:


#Plota os resultados
erro_day.collect()


# ### 5. O total de bytes retornados.

# In[19]:


# Separa os dados de bytes recebidos
df_bytes = textFile.map(lambda line: line.split(" ")[9:10])


# In[22]:


# Remove os dados invalidos
df_bytes_t = df_bytes.filter(lambda x: "-" not in x)


# In[ ]:


# soma os bytes recebidos
df_bytes_t.reduce(lambda x, y: x + y).collect()


# Não consegui realizar a contagem de bytes recebidos, a contagem demorou bastante e crachou 
