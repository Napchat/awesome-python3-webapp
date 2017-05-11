# awesome-python3-webapp
practical using of python

```python
import time
from flask import Flask
from flask_common import Common

app = Flask(__name__)
app.debug = True

common = Common(app)

@app.route("/")
@common.cache.cached(timeout=50)
def hello():
    time.sleep(1)
    return "Hello World!"


if __name__ == "__main__":
    common.serve()
```
