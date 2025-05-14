from app import create_app

app = create_app()

if __name__ == '__main__':
    # Para desarrollo
    app.run(host='0.0.0.0', port=8080, debug=True)
    
    # Para producción, comenta la línea anterior y descomenta esta:
    # app.run(host='0.0.0.0', port=8080, ssl_context='adhoc')