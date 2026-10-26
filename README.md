# Bootcamp de Big Data, Inteligencia Artificial y Machine Learning

Prácticas que realicé durante el bootcamp, desde los fundamentos de programación y matemáticas hasta el *fine-tuning* de modelos de lenguaje y el despliegue de modelos en producción.

**Stack:** Python · SQL · Scala · Spark · R · scikit-learn · PyTorch · Hugging Face · MLflow · FastAPI · Power BI

---

## Prácticas destacadas

| Práctica | Qué hice | Resultado |
|---|---|---|
| [Deep Learning](DeepLearning/PRACTICA_DL) | Modelo **multimodal** que combina la imagen de un punto de interés turístico (CNN EfficientNet-B0) con sus metadatos (red neuronal) para predecir si tendrá un *engagement* alto | Accuracy del **77,5%**, equilibrado entre ambas clases |
| [NLP](NLP) | Clasificación de sentimiento de reseñas de Amazon (deportes y aire libre): exploración con nubes de palabras, preprocesado y comparación de modelos con TF-IDF y Word2Vec | Accuracy del **83,8%** |
| [Machine Learning](MachineLearning) | Predicción del precio de alojamientos de Airbnb: limpieza, *target encoding*, regularización con Lasso y modelos de árboles con búsqueda de hiperparámetros | R² de **0,79** en test |
| [Despliegue de algoritmos](Despligue_algoritmos/Practica_Despliegue_Algoritmos) | Ciclo de vida de un modelo con **MLflow** (seguimiento de experimentos, registro y paso a producción) y una **API con FastAPI** que sirve modelos de análisis de sentimiento y generación de texto | Modelo registrado y API funcionando |
| [IA generativa](IA) | *Fine-tuning* del modelo de lenguaje **Gemma 2B** con QLoRA y Unsloth para clasificar noticias (dataset AG News) | Entrenamiento eficiente ajustando menos del 1% de los parámetros |

## Todos los módulos

| Módulo | Práctica | Tecnologías |
|---|---|---|
| [Programación 101](Programacion101) | Compresor de matrices: cada celda se sustituye por el promedio de sus vecinas | Python |
| [Matemáticas 101](Matematicas101) | Regresión lineal por mínimos cuadrados en forma matricial (dataset Auto MPG), compresión de imágenes con SVD y análisis estadístico de alojamientos de Airbnb en Madrid | Python, NumPy, R, Quarto |
| [SQL](SQL) | Análisis de datos de vuelos: estados, retrasos, actualizaciones y aeropuertos de origen | SQL |
| [Visualización de datos](VisualizaciónDatos) | Cuadro de mando interactivo | Power BI |
| [Arquitectura Big Data](ArquitecturaBigData) | Diseño de una arquitectura de datos (memoria en PDF) | Arquitectura de datos |
| [Big Data Processing](BigDataProcessing) | DataFrames, UDF, *joins*, agregaciones y RDDs | Scala, Apache Spark |
| [Machine Learning](MachineLearning) | Predicción del precio de Airbnb | scikit-learn, pandas |
| [Deep Learning](DeepLearning/PRACTICA_DL) | Clasificador multimodal imagen + metadatos | PyTorch, torchvision |
| [NLP](NLP) | Análisis de sentimiento de reseñas | NLTK, spaCy, Gensim, scikit-learn |
| [IA generativa](IA) | *Fine-tuning* de Gemma 2B con QLoRA | Hugging Face, Unsloth, TRL |
| [Despliegue de algoritmos](Despligue_algoritmos/Practica_Despliegue_Algoritmos) | MLflow + API REST | MLflow, FastAPI, Transformers |

---

## Proyectos personales

- [**¿Chollo o timo? Detector de pisos mal tasados en Valencia**](https://github.com/fernandoamalfitano/chollo-o-timo-valencia): modelo LightGBM que estima el precio justo de cada vivienda y explica con SHAP por qué un anuncio está por encima o por debajo de lo que debería costar.

---

**Fernando Amalfitano**
[LinkedIn](https://www.linkedin.com/in/fernando-amalfitano-051751388/) · [Email](fernandoamalfitano@gmail.com)

