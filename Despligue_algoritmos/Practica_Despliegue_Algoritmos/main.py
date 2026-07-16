from fastapi import FastAPI
from transformers import pipeline

app = FastAPI(title="Práctica Despliegue")

sentiment = pipeline("sentiment-analysis")
generator = pipeline("text-generation")

@app.get("/")
def inicio():
    return {"mensaje": "API de práctica"}

@app.get("/saludo/{nombre}")
def saludo(nombre: str):
    return {"saludo": f"Hola {nombre}"}

@app.get("/suma")
def suma(a: float, b: float):
    return {"resultado": a + b}

@app.get("/sentimiento/{texto}")
def sentimiento(texto: str):
    return sentiment(texto)

@app.get("/generar/{texto}")
def generar(texto: str):
    return generator(texto, max_new_tokens=30)