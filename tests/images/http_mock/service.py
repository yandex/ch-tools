import json

from flask import Flask

app = Flask(__name__)


@app.route("/computeMetadata/v1/instance/service-accounts/default/token")
def token():
    return json.dumps(
        {"access_token": "IAM_TOKEN", "expires_in": 0, "token_type": "Bearer"}
    )


@app.route("/")
def ping():
    return "OK"
