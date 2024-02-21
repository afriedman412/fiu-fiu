from src import app
import os


if __name__ == "__main__":
    if 'PRODUCTION' in os.environ:
        app.run(host="0.0.0.0", port=5000)
    else:
        app.run(host="0.0.0.0", port=5000, debug=True)
