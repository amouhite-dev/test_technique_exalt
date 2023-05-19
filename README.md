## Explication de l'architecture
NB: L'API de FlightRadarAPI24 n'existe plus je crois. Parce que, malgré que j'ai souscris à la période d'essai, j'ai pu avoir d'access key. J'ai donc recouru a une autre API ; celui-ci :  https://aviationstack.com/documentation ;pour réaliser le test.
#### Schéma de l'architecture

[Architecture ETL FLights EXALT IT](media-assets/Architecture_ETL_Flights_ExaltIT.png "Architecture ETL FLights EXALT IT")

#### Explication générale de l'architecture

Concernant cet architecture, nous sommes sur un ETL qui tournera sur Docker.

Le dockerfile contiendra, l'image de AIRFLOW par exemple apache/apache/airflow:2.3.0. 
Comme exemple de dockerfile nous pouvons avoir celui-ci:

    FROM apache/airflow:2.3.0
    ARG AIRFLOW_USER_HOME=/opt/airflow
    ENV PYTHONPATH=$PYTHONPATH:${AIRFLOW_USER_HOME}

    USER airflow

    COPY . ${AIRFLOW_USER_HOME}

    RUN pip install -r requirements.txt 

Le premier docker-compose contiendra tout les service de AIrflow et le second ceux de Kafka
NB: Le requirements.txt, contiendra tout les libraires dont ont a besoin pour lancer notre projet sur docker

Dans notre environmment Airflow, nous allons créé un DAG qui va tourner tout les 2 heures. 
Ex:

    # initialization of the default arguments of a DAG
    default_args = {
        'owner' : 'madebo',
        'start_date' : datetime(2022,10,19),
        'retries' : 5,
        'retry_delay' : timedelta(minutes=5),
        'email': ['amouhite2002@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
    }

    # defined the DAG
    dag = DAG (
        'etl-flight-by-hour',
        default_args=default_args, 
        description = 'DAG devant extraire des vols chaque 2h sur l'API de aviationstack',
        schedule_interval= '0 */2 * * *',
        catchup=False
    )

    with dag:
        starter_op = DummyOperator(task_id = "Start_crawling_professions_directories")
    
        extraction_op = PythonOperator(
            task_id='extraction',
            python_callable=<nom de la function_extraction>,
            dag=dag,
        )  

        transformation_op = PythonOperator(
            task_id='transformation',
            python_callable=<nom de la function_transformation>,
            dag=dag,
        )  

        save_data_op = PythonOperator(
            task_id='save_data',
            python_callable=<nom de la function_save_data>,
            dag=dag,
        )  
        
    extraction_op >> transformation_op >> save_data_op
        

Ensuite j'ai rajouter Kafka qui lui permettra d'alimenter une API, créé avec django (librairie DRF) en temps réel. Cet API sera une API utiliser par les data analyst pour faire du reporting par exemple.

Kafka envoie mes données en produisant un méssage dans le brocker en passant par le  topics qui lui à été assigné. Le brocker Kafka sera aussi appeler par le consumer Kafka pour consommer le message envoyer dans le brocker à travers le topics qu'on lui a assigner. C'est ainsi que l'API est ainsi alimenter en temps réel.

Tout ce procéder sera monitorer avec promethus, afin de surveiller l'exécution du pipeline.

#### Explication Particulière

1. **Base de données**
Pour la base de données, j'ai choisi une base de données NoSQL, parce que nous recevons à près de 99% de nouvelle données a chaque extraction et ces derniers n'ont pas souvent le même format.
Deplus, étant donnés qu'il est évolutive, il pourra facilement gérer le grand volume de données qu'il recevra au bout de 1mois par exemple (tout cela parce qu'on fait l'xtraction chaque 2h, avec 99% de chance d'avoir de nouvelle données)
Contrairement à celui du base de données SQL, les requêtes pourront être rapide, vue qu'on serait améné a récupérer un grand volume de données à chaque requête.

2. **Monitoring**
J'ai choisi promethus, parce que c'est un outils qui facile à intégrer et aussi pour son systeme d'alerte intégrer , qui permet d'être prévenu vers diverses destinations.