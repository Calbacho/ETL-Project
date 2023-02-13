# W4 Project - ETL Project
ETL para proyecto de predicción de accidentes de tráfico en Madrid

![portada](https://github.com/Calbacho/ETL-Project/blob/main/img/trafico.jpg)

## ⛓️ Índice
[1. 🔎 Objetivo](#objetivo)\
[2.🚨 Histórico de accidentes](#accidentes)\
[3.🚦 Puntos de medida](#puntos)\
[4.🚘 Histórico de tráfico](#trafico)\
[5.✅ Carga](#carga)\
[6.🔲 Próximos pasos](#carga)

<a name="objetivo"/>

## 🔎 Objetivo

Fase **ETL** para proyecto de predicción de accidentes de tráfico en Madrid según estadísticas macro como la localización, el momento, las condiciones de tráfico y las condiciones meteorológicas.

**Extracción:**
- Histórico de accidentes
- Puntos de medida
- Histórico de tráfico

**Limpieza:**
- Nulos
- Duplicados
- Consistencia
- Fromato
- Nuevas columnas

**Carga:**
- A mi disco duro
- A SQL
- A GitHub

<a name="accidentes"/>

## 🚨 Histórico de accidentes

10 CSVs con los accidentes cada año
```
acc_2013 = pd.read_csv('2013_Accidentalidad.csv', sep = ';', encoding="ISO-8859-1")
```

### Columnas

**¿Cuándo?** Fecha, Hora, Día de la semana

**¿Dónde?** Distrito, Localización, Coordenadas

**¿Gravedad?** Ileso, Leve, Grave, Fallecidos...

**¿Condiciones?** Meteorología

### Limpieza

- Nulos
- Duplicados
- Consistencia
- Fromato
- Nuevas columnas


```
345176 rows × 10 columns
```


**¿Podría encontrar el nivel de tráfico antes del accidente?**

<a name="puntos"/>

## 🚦 Puntos de medida

<details>
<summary>¿Qué son?</summary>
<br>

Estos sistemas de detección son, en su mayoría, lazos electromagnéticos que se colocan
debajo del pavimento y detectan la masa metálica de los vehículos que pasan sobre ellos,
siendo sistemas de gran calidad y precisión.
  
![lazo](https://github.com/Calbacho/ETL-Project/blob/main/img/lazo.jpg)

</details>

<details>
<summary>¿Qué miden?</summary>
<br>

**1. Intensidad:** Nº de vehículos en los últimos 15 minutos

**2. Ocupación:** Porcentage de vía ocupada por vehículos en los últimos 15 minutos
  
**3. Velocidad media** en los últimos 15 minutos

</details>

<details>
<summary>¿Dónde están?</summary>
<br>

![geo_ejem](https://github.com/Calbacho/ETL-Project/blob/main/img/geo_ejem.png)

```
geo_trackers = pd.read_csv('pmed_ubicacion_12-2022.csv', sep = ';', on_bad_lines='skip')
```

<img src="https://github.com/Calbacho/ETL-Project/blob/main/img/tabla_geos.png" width="300" height="300" />

</details>




<a name="trafico"/>

## 🚘 Histórico de tráfico

**Web Scrapping** para navegar por url, descargar 96 zips y extraerlos a csv.

**Vaex** para leer cada CSV, concatenarlo por año, limpiar cada año y generar tabla histórica.

<details>
<summary>CÓDIGO</summary>
<br>


#### Abrir página web
  
```
driver.get(url)

time.sleep(2)
```
  
#### Aceptar cookies
  
```
cookies = driver.find_element(By.XPATH, '//*[@id="iam-cookie-control-modal-action-primary"]')

try:
    cookies.click()
except:
    pass

time.sleep(2)
```
  
#### Ir a página con todos los zips

```
enlaces = driver.find_element(By.XPATH, '//*[@id="readspeaker"]/div[3]/div/div[1]/div/a')

try:
    enlaces.click()
except:
    pass

time.sleep(2)
```
 
#### Meter los enlaces a zips en una lista

```
links = driver.find_elements(By.XPATH, '//*[@id="readspeaker"]/div[3]/div/div[1]/ul/li/ul/li/ul/li/a')[:96]

urls  = []

for i in links:
    urls.append(i.get_attribute('href'))
```
  
#### Descargar y extraer

```
for i in urls:
    
    count += 1
    
    r = requests.get(i)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall('zips')
```

#### Convertir a formato Vaex

```
path = os.getcwd()

csv_files = glob.glob(os.path.join(path + '\\zips', "*.csv"))


for i in csv_files:

    df_temp = vaex.from_csv(i, sep = ';', convert = True, chunk_size = 5_000_000, on_bad_lines='skip', low_memory=False) 
```

#### Abrir por año y concatenar

```
df_2015 = vaex.open('C:\\Users\\calba\\OneDrive\\Desktop\\zips\\01-2015.csv.hdf5')

vaex_files = glob.glob(os.path.join(path + '\\zips', "*.hdf5"))

for i in vaex_files[0::8][1:-1]:
    df = vaex.open(i)
    
    df_2015 = df_2015.concat(df)
```

</details>

```
1159303428 rows × 6 columns
```

<a name="carga"/>

## ✅ Carga

- A mi disco duro
- A SQL
<img src="https://github.com/Calbacho/ETL-Project/blob/main/img/sql_acc.png" width="450" height="450" />

- A GitHub

<a name="pasos"/>

## 🔲 Próximos pasos

1. **Enriquecimiento** de los datos - ¿Viento? ¿Discotecas cercanas? ¿Colegios cercanos? ¿Eventos?
2. **Análisis descriptivo** de los datos
3. **ML** modelo de predicción


