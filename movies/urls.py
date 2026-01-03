from django.urls import path
from . import views

urlpatterns = [
    # Accueil
    path('', views.home, name='home'),
    
    # Catalogue
    path('movies/', views.movie_list, name='movie_list'),
    
    # Détail (capture l'ID, ex: tt0012345)
    path('movies/<str:movie_id>/', views.movie_detail, name='movie_detail'),
    
    # Recherche & Stats
    path('search/', views.search, name='search'),
    path('stats/', views.stats_view, name='stats'),
]