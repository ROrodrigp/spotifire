document.addEventListener('DOMContentLoaded', function() {
    // Función para actualizar los datos del dashboard
    function actualizarDatos() {
        // Mostrar indicador de carga
        mostrarCargando(true);
        
        // Hacer petición AJAX para actualizar datos
        fetch('/actualizar_datos', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            credentials: 'same-origin' // Incluir cookies en la petición
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Error al actualizar datos');
            }
            return response.json();
        })
        .then(data => {
            // Actualizar lista de canciones recientes
            actualizarCanciones(data.recent_tracks);
            
            // Actualizar lista de artistas top
            actualizarArtistas(data.top_artists);
            
            // Mostrar mensaje de éxito
            mostrarMensaje('Datos actualizados correctamente', 'success');
        })
        .catch(error => {
            console.error('Error:', error);
            mostrarMensaje('Error al actualizar los datos: ' + error.message, 'danger');
        })
        .finally(() => {
            // Ocultar indicador de carga
            mostrarCargando(false);
        });
    }
    
    // Función para actualizar la lista de canciones recientes
    function actualizarCanciones(canciones) {
        const contenedor = document.querySelector('.card:nth-of-type(1) .card-body');
        if (!contenedor || !canciones || canciones.length === 0) return;
        
        // Limpiar contenedor
        contenedor.innerHTML = '';
        
        // Añadir cada canción al contenedor
        canciones.forEach(cancion => {
            const elemento = document.createElement('div');
            elemento.className = 'track-item';
            elemento.innerHTML = `
                <div class="track-details">
                    <div class="track-title">${cancion.name}</div>
                    <div class="track-artist">${cancion.artist}</div>
                </div>
                <div class="track-time">
                    ${cancion.played_at || ''}
                </div>
            `;
            contenedor.appendChild(elemento);
        });
    }
    
    // Función para actualizar la lista de artistas top
    function actualizarArtistas(artistas) {
        const contenedor = document.querySelector('.card:nth-of-type(2) .card-body');
        if (!contenedor || !artistas || artistas.length === 0) return;
        
        // Limpiar contenedor
        contenedor.innerHTML = '';
        
        // Añadir cada artista al contenedor
        artistas.forEach(artista => {
            const elemento = document.createElement('div');
            elemento.className = 'artist-item';
            
            // Determinar los géneros a mostrar
            const generos = artista.genres && artista.genres.length > 0 
                ? artista.genres.join(', ') 
                : 'Sin géneros definidos';
            
            // Determinar la clase de popularidad (redondear a la decena más cercana)
            const popularidad = artista.popularity !== undefined 
                ? Math.floor(artista.popularity / 10) * 10 
                : 50;
            
            elemento.innerHTML = `
                <div class="artist-details">
                    <div class="artist-name">${artista.name}</div>
                    <div class="artist-genre">${generos}</div>
                    <div class="popularity-bar popularity-${popularidad}"></div>
                </div>
            `;
            contenedor.appendChild(elemento);
        });
    }
    
// Función para mostrar mensajes de alerta
function mostrarMensaje(mensaje, tipo) {
    // Crear elemento de alerta
    const alerta = document.createElement('div');
    alerta.className = `alert alert-${tipo} alert-dismissible fade show`;
    alerta.role = 'alert';
    alerta.innerHTML = `
        ${mensaje}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Insertar al principio del área de contenido
    const contenido = document.querySelector('.content-area');
    if (contenido) {
        contenido.insertBefore(alerta, contenido.firstChild);
        
        // Autodestruir después de 5 segundos
        setTimeout(() => {
            alerta.classList.remove('show');
            setTimeout(() => alerta.remove(), 150);
        }, 5000);
    }
}

// Función para mostrar/ocultar indicador de carga
function mostrarCargando(mostrar) {
    const botonActualizar = document.querySelector('.welcome-banner button');
    if (!botonActualizar) return;
    
    if (mostrar) {
        botonActualizar.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Actualizando...';
        botonActualizar.disabled = true;
    } else {
        botonActualizar.innerHTML = 'Actualizar datos';
        botonActualizar.disabled = false;
    }
}

// Asignar evento al botón de actualizar
const botonActualizar = document.querySelector('.welcome-banner button');
if (botonActualizar) {
    botonActualizar.addEventListener('click', actualizarDatos);
}

// Configurar interactividad del menú lateral en móviles
const sidebarItems = document.querySelectorAll('.sidebar-item');
sidebarItems.forEach(item => {
    item.addEventListener('click', function(e) {
        // Si estamos en móvil y no es un enlace con href, prevenir navegación
        if (window.innerWidth < 992 && (!this.href || this.href === '#')) {
            e.preventDefault();
        }
        
        // Quitar clase active de todos los elementos
        sidebarItems.forEach(i => i.classList.remove('active'));
        
        // Añadir clase active al elemento clickeado
        this.classList.add('active');
    });
});
});