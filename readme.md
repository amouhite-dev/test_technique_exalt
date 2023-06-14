***version: français***
#### Sujet
Créer un pipeline ***ETL (Extract, Transform, Load)*** permettant de traiter les données de ***l'[API aviationstack](https://aviationstack.com/)***, qui répertorie l'ensemble des ***vols aériens, aéroports, compagnies aériennes mondiales***.

C'est une API est payant, j'ai utilisant la période d'essai pour réaliser le projet. Pour la version à laquel j'ai souscrire, je ne peux que récupérer 100 données et faire 10000 requêtes par mois.

#### Tâches
- Extraction de données avec la librairie python "requests"
- Transformation de données:
  - Data preparation : 
    1. Créer un cadre de données pour chaque extrait d'ensemble de données
    2. Explosez mon dataframe df_flights: [Explication: parce que pour certains de mes colonnes j'avais des sous structure de données]
    3. Affichage de la taille des données et du premier élement : [Explication: C'est pour connaitre la taille des données extraites et avoir une vue de ceux à quoi elle ressemble]
    4. Néttoyage des données
  
        4.1. Convertir le type de chaque colonne [Explication: Leur type etait par défaut, mais certains données nécessitait d'avoir leur vrai type comme les dates]

        4.2. Remplir les valeur NaN [Explication: Pour pouvoir mieux faire mon traitement de données]

        4.3. Séparer les colonnes departure_timezone et arrival_timezone du continent et de la ville. [Explication: Il serait plus simple d'avoir les deux éléments séparer , car les résultats seront différents une fois séparer par rapport à lorsqu'il était ensemble]

        4.4. Calculer la durée en secondes et en heures des vols [Explication: Cela pourrai nous servir lors de notre traitement ou plus tard les data analyst par exemple]
  - Requêtage : 
    1. La compagnie avec le + de vols en cours
    2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
    3. Le vol en cours avec le trajet le plus long
    4. Pour chaque continent, la longueur de vol moyenne
    5. L'entreprise constructeur d'avions avec le plus de vols actifs
    6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
- Enregistrer la données extraites

***NB: J'ai commenté tout mon code, afin que le lecteur puisse comprendre ce que j'ai fait***

> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

***version anglaise***
#### Topic
Create a ***ETL (Extract, Transform, Load)*** pipeline to process data from ***the [aviationstack API](https://aviationstack.com/)***, which lists all ***air flights, airports, airlines worldwide***.

It is an API is paying, I used the trial period to realize the project. For the version I subscribed to, I can only retrieve 100 data and make 10000 queries per month.

#### Tasks
- Data extraction with the python library "requests
- Data transformation:
  - Data preparation : 
    1. Create a data frame for each data set extract
    2. Explain my dataframe df_flights: [Explanation: because for some of my columns I had sub data structures]
    3. Display data size and first element: [Explanation: This is to know the size of the extracted data and have a view of what it looks like]
    4. Data cleanup
  
        4.1. Convert the type of each column [Explanation: Their type was default, but some data needed to have their real type like dates]

        4.2 Fill in the NaN values [Explanation: To be able to do my data processing better]

        4.3. Separate the departure_timezone and arrival_timezone columns from the continent and city. [Explanation: It would be easier to have the two elements separated, because the results will be different when separated than when they were together].

        4.4 Calculate the duration in seconds and hours of the flights [Explanation: This could be useful for our processing or later the data analysts for example].
  - Query : 
    1. The company with the most flights in progress
    2. For each continent, the airline with the most active regional flights (continent of origin == continent of destination)
    3. The current flight with the longest route
    4. For each continent, the average flight length
    5. The aircraft manufacturer with the most active flights
    6. For each airline country, the top 3 aircraft models in use
- Save the extracted data

***NB: I commented all my code, so the reader can understand what I did***