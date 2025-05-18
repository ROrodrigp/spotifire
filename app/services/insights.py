"""
Servicio para generar insights basados en datos históricos del usuario.
Temporalmente utiliza datos simulados hasta que se implemente la base de datos.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import Config

logger = logging.getLogger(__name__)

class InsightsService:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.user_dir = os.path.join(Config.USERS_DATA_DIR, user_id)
        
    def get_available_insights(self) -> Dict:
        """
        Determina qué insights están disponibles basándose en los datos recolectados.
        """
        # Simular día de registro del usuario (en producción esto vendrá de la BD)
        registration_date = self._get_user_registration_date()
        days_since_registration = (datetime.now() - registration_date).days
        
        insights = {
            'basic_patterns': {
                'available': days_since_registration >= 3,
                'unlock_in_days': max(0, 3 - days_since_registration),
                'name': 'Patrones básicos de escucha',
                'description': 'Descubre a qué horas del día escuchas más música'
            },
            'genre_distribution': {
                'available': days_since_registration >= 7,
                'unlock_in_days': max(0, 7 - days_since_registration),
                'name': 'Distribución de géneros musicales',
                'description': 'Análisis de los géneros que más escuchas'
            },
            'weekly_trends': {
                'available': days_since_registration >= 14,
                'unlock_in_days': max(0, 14 - days_since_registration),
                'name': 'Tendencias semanales',
                'description': 'Compara tu actividad musical entre diferentes semanas'
            },
            'artist_discovery': {
                'available': days_since_registration >= 21,
                'unlock_in_days': max(0, 21 - days_since_registration),
                'name': 'Descubrimiento de artistas',
                'description': 'Análisis de cómo descubres nuevos artistas'
            }
        }
        
        return insights
    
    def get_dashboard_insights(self) -> List[Dict]:
        """
        Obtiene los insights destacados para mostrar en el dashboard principal.
        """
        available_insights = self.get_available_insights()
        dashboard_insights = []
        
        # Insight 1: Patrones horarios
        if available_insights['basic_patterns']['available']:
            dashboard_insights.append(self._generate_hourly_patterns_insight())
        else:
            dashboard_insights.append({
                'type': 'locked',
                'title': available_insights['basic_patterns']['name'],
                'description': available_insights['basic_patterns']['description'],
                'unlock_in_days': available_insights['basic_patterns']['unlock_in_days'],
                'progress': self._calculate_progress(3, available_insights['basic_patterns']['unlock_in_days'])
            })
        
        # Insight 2: Géneros
        if available_insights['genre_distribution']['available']:
            dashboard_insights.append(self._generate_genre_distribution_insight())
        else:
            dashboard_insights.append({
                'type': 'locked',
                'title': available_insights['genre_distribution']['name'],
                'description': available_insights['genre_distribution']['description'],
                'unlock_in_days': available_insights['genre_distribution']['unlock_in_days'],
                'progress': self._calculate_progress(7, available_insights['genre_distribution']['unlock_in_days'])
            })
        
        # Insight 3: Actividad semanal (solo si hay suficientes datos)
        if available_insights['weekly_trends']['available']:
            dashboard_insights.append(self._generate_weekly_activity_insight())
        elif len(dashboard_insights) < 3:
            dashboard_insights.append({
                'type': 'locked',
                'title': available_insights['weekly_trends']['name'],
                'description': available_insights['weekly_trends']['description'],
                'unlock_in_days': available_insights['weekly_trends']['unlock_in_days'],
                'progress': self._calculate_progress(14, available_insights['weekly_trends']['unlock_in_days'])
            })
        
        return dashboard_insights[:3]  # Máximo 3 insights en el dashboard
    
    def _get_user_registration_date(self) -> datetime:
        """
        Obtiene la fecha de registro del usuario.
        Temporalmente simula que se registró hace 2 días.
        """
        # En producción, esto vendría de la base de datos
        # Por ahora, simular que el usuario se registró hace 2 días
        return datetime.now() - timedelta(days=2)
    
    def _calculate_progress(self, total_days: int, days_remaining: int) -> int:
        """Calcula el porcentaje de progreso hacia desbloquear un insight."""
        days_completed = total_days - days_remaining
        return min(100, max(0, int((days_completed / total_days) * 100)))
    
    def _generate_hourly_patterns_insight(self) -> Dict:
        """
        Genera un insight sobre patrones horarios de escucha.
        """
        # Datos simulados - en producción esto vendría de la BD
        peak_hour = 14  # 2 PM
        peak_activity = 85
        
        # Generar datos simulados por hora
        hourly_data = []
        for hour in range(24):
            # Simular más actividad en ciertas horas
            if 8 <= hour <= 10:  # Mañana
                activity = 60 + (hour - 8) * 10
            elif 12 <= hour <= 16:  # Tarde
                activity = 70 + (15 - abs(hour - 14)) * 5
            elif 18 <= hour <= 22:  # Noche
                activity = 50 + (20 - abs(hour - 20)) * 8
            else:
                activity = 20 + abs(12 - hour) * 2
            
            hourly_data.append({
                'hour': hour,
                'activity': min(100, activity + (hash(str(hour)) % 20 - 10))
            })
        
        return {
            'type': 'hourly_patterns',
            'title': 'Tu hora pico de escucha',
            'description': f'Escuchas más música alrededor de las {peak_hour}:00',
            'data': hourly_data,
            'peak_hour': peak_hour,
            'peak_activity': peak_activity
        }
    
    def _generate_genre_distribution_insight(self) -> Dict:
        """
        Genera un insight sobre distribución de géneros.
        """
        # Datos simulados - en producción esto vendría de la BD
        genres = [
            {'name': 'Pop', 'percentage': 35, 'color': 'bg-green-500'},
            {'name': 'Rock', 'percentage': 25, 'color': 'bg-blue-500'},
            {'name': 'R&B', 'percentage': 20, 'color': 'bg-purple-500'},
            {'name': 'Hip Hop', 'percentage': 15, 'color': 'bg-yellow-500'},
            {'name': 'Otros', 'percentage': 5, 'color': 'bg-gray-500'}
        ]
        
        top_genre = genres[0]
        
        return {
            'type': 'genre_distribution',
            'title': f'Tu género favorito: {top_genre["name"]}',
            'description': f'{top_genre["percentage"]}% de tu música es {top_genre["name"]}',
            'data': genres,
            'top_genre': top_genre['name']
        }
    
    def _generate_weekly_activity_insight(self) -> Dict:
        """
        Genera un insight sobre actividad semanal.
        """
        # Datos simulados - en producción esto vendría de la BD
        current_week_minutes = 420  # 7 horas
        previous_week_minutes = 350  # 5.8 horas
        change_percentage = ((current_week_minutes - previous_week_minutes) / previous_week_minutes) * 100
        
        return {
            'type': 'weekly_activity',
            'title': 'Actividad semanal',
            'description': f'{"↑" if change_percentage > 0 else "↓"} {abs(change_percentage):.1f}% vs semana anterior',
            'current_week_minutes': current_week_minutes,
            'previous_week_minutes': previous_week_minutes,
            'change_percentage': change_percentage,
            'trend': 'up' if change_percentage > 0 else 'down'
        }
    
    def get_next_milestone(self) -> Optional[Dict]:
        """
        Obtiene información sobre el próximo hito que se desbloqueará.
        """
        available_insights = self.get_available_insights()
        
        # Encontrar el próximo insight que se desbloqueará
        next_insight = None
        min_days = float('inf')
        
        for insight_key, insight_data in available_insights.items():
            if not insight_data['available'] and insight_data['unlock_in_days'] < min_days:
                min_days = insight_data['unlock_in_days']
                next_insight = insight_data
        
        if next_insight:
            return {
                'name': next_insight['name'],
                'description': next_insight['description'],
                'unlock_in_days': next_insight['unlock_in_days']
            }
        
        return None