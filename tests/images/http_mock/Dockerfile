FROM python:3-alpine
RUN pip3 install flask --no-cache
ENV FLASK_APP=/service.py
COPY tests/images/http_mock/service.py /
CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0", "--port=8080"]
