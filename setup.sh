#!/bin/bash

# Colores para mensajes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Función para imprimir mensajes
print_message() {
    echo -e "${BLUE}==>${NC} $1"
}

# Verificar si requirements.txt existe
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}Error: No se encontró el archivo requirements.txt${NC}"
    echo "Generando requirements.txt desde el entorno actual..."
    
    # Verificar si estamos en un entorno virtual
    if [ -z "$VIRTUAL_ENV" ]; then
        echo -e "${RED}Error: No estás en un entorno virtual.${NC}"
        echo "Por favor, activa tu entorno virtual con 'source venv/bin/activate'"
        echo "y luego ejecuta 'pip freeze > requirements.txt'"
        exit 1
    fi
    
    pip freeze > requirements.txt
    echo -e "${GREEN}requirements.txt generado correctamente.${NC}"
fi

# Crear entorno virtual si no existe
if [ -d "venv" ]; then
    print_message "Se encontró un entorno virtual. ¿Deseas recrearlo? [s/N]"
    read -r choice
    if [ "$choice" = "s" ] || [ "$choice" = "S" ]; then
        print_message "Eliminando entorno virtual existente..."
        rm -rf venv
        print_message "Creando nuevo entorno virtual..."
        python3 -m venv venv
    else
        print_message "Usando entorno virtual existente."
    fi
else
    print_message "Creando entorno virtual..."
    python3 -m venv venv
fi

# Actualizar pip e instalar dependencias
print_message "Instalando dependencias..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Crear directorios necesarios
print_message "Creando estructura de directorios..."
mkdir -p data/users_data
mkdir -p static/css static/js
mkdir -p templates/auth templates/dashboard

# Configurar permisos
print_message "Configurando permisos..."
chmod +x run.sh
if [ -f "scripts/spotify_periodic_collector.py" ]; then
    chmod +x scripts/spotify_periodic_collector.py
fi
chmod -R 755 app/ static/ templates/
chmod -R 770 data/users_data/

# Crear .env si no existe y existe .env.example
if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    print_message "Creando archivo .env a partir de .env.example..."
    cp .env.example .env
    echo -e "${GREEN}Archivo .env creado. Edítalo con tus configuraciones.${NC}"
fi

# Mensaje final
echo -e "\n${GREEN}Configuración completada con éxito.${NC}"
echo -e "\nPara activar el entorno virtual, ejecuta:"
echo -e "${BLUE}source venv/bin/activate${NC}"
echo -e "\nPara ejecutar la aplicación en modo desarrollo:"
echo -e "${BLUE}python run.py${NC}"
echo -e "\nPara ejecutar la aplicación en modo producción:"
echo -e "${BLUE}./run.sh${NC}"