from omsserver import app, socketio

if __name__ == "__main__":
    socketio.run(app, host='127.0.0.1', port=5000, debug=True)

