from django.shortcuts import render
import json # Nécessaire pour les graphiques Chart.js

# Import des services SQLite
from .services.sqlite_service import (
    get_sqlite_stats, 
    get_top_movies,
    get_random_movies, 
    get_movies_list, 
    get_all_genres,
    get_stats_for_charts # Nouvelle fonction pour les graphiques
    
)

# Import des services MongoDB (C'est cette ligne qui manquait !)
from .services.mongo_service import (
    get_movie_details, 
    get_similar_movies, 
    get_mongo_stats
)

def home(request):
    """Page d'accueil avec Top films."""
    global_stats = get_sqlite_stats()
    top_movies = get_top_movies(10)
    
    random_movies = get_random_movies(5)
    
    context = {
        'stats': global_stats,
        'movies': top_movies,
        'random_movies': random_movies # <--- On l'envoie au template
    }
    
    return render(request, 'movies/home.html', context)

# Ajoute cette petite fonction utilitaire juste avant movie_list
def clean_param(param):
    """Transforme 'None' (texte) ou chaîne vide en None (objet)"""
    if param in [None, 'None', '']:
        return None
    return param

def movie_list(request):
    """Catalogue avec filtres et pagination."""
    page = int(request.GET.get('page', 1))
    
    # On nettoie chaque paramètre pour éviter le bug du "None"
    filters = {
        'q': clean_param(request.GET.get('q')),
        'genre': clean_param(request.GET.get('genre')),
        'year': clean_param(request.GET.get('year')),
        'rating': clean_param(request.GET.get('rating')),
        'sort': request.GET.get('sort', 'year_desc') # Valeur par défaut ici
    }

    # Le reste ne change pas
    movies, has_next = get_movies_list(page=page, filters=filters)
    genres = get_all_genres()
    
    context = {
        'movies': movies, 
        'genres': genres, 
        'filters': filters, 
        'page': page, 
        'has_next': has_next,
        'next_page': page + 1, 
        'prev_page': page - 1
    }
    return render(request, 'movies/movie_list.html', context)

def movie_detail(request, movie_id):
    """Page Détail (Source: MongoDB)."""
    # 1. On cherche le film dans Mongo
    movie = get_movie_details(movie_id)
    
    # 2. Gestion erreur 404 si pas trouvé
    if not movie:
        return render(request, 'movies/404.html', {'message': f"Le film {movie_id} est absent de MongoDB"}, status=404)

    # 3. Films similaires
    genres = movie.get('genres', [])
    similar_movies = get_similar_movies(genres, exclude_id=movie_id)

    return render(request, 'movies/movie_detail.html', {'movie': movie, 'similar_movies': similar_movies})

def search(request):
    return movie_list(request)

def stats_view(request):
    """Page Statistiques avec Graphiques Chart.js."""
    # On récupère les données brutes SQL
    chart_data = get_stats_for_charts()
    
    # On les convertit en JSON pour que le JavaScript puisse les lire
    context = {
        'sqlite': get_sqlite_stats(),
        'mongo': get_mongo_stats(),
        'genres_data': json.dumps(chart_data['genres']),
        'decades_data': json.dumps(chart_data['decades']),
        'ratings_data': json.dumps(chart_data['ratings']),
        'actors_data': json.dumps(chart_data['actors'])
    }
    return render(request, 'movies/stats.html', context)