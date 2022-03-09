FROM ch-tools-tests-base

RUN pip3 install flask
ENV FLASK_APP /service.py
COPY images/http_mock/service.py /

EXPOSE 8080

CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0", "--port=8080"]