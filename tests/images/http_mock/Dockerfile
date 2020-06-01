FROM python:3

RUN pip install flask
ENV FLASK_APP /service.py
COPY images/http_mock/service.py /
CMD ["python", "-m", "flask", "run", "--host=0.0.0.0", "--port=8080"]