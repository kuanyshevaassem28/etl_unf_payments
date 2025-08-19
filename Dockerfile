FROM python:3.11-slim

# Системные зависимости (минимально)
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала зависимости для кэша
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Затем код
COPY . .

# Запуск приложения
CMD ["python", "main.py"]
