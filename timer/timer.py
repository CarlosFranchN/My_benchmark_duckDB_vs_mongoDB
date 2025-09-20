import time
import functools

def medir_tempo(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args,**kwargs)
        fim = time.time()
        duracao = fim - inicio
        wrapper.duracao = round(duracao,4)
        # print(f"Duração de tempo: {duracao:.4f} segundos")
        return resultado
    wrapper.duracao = 0
    return wrapper