-- ============================================================================
-- Schema en etoile GOLD (Coupe du Monde 1930-2022) — servi par PostgreSQL.
-- Dialecte : PostgreSQL.  Schema : gold  (search_path = gold cote connexion).
--
-- NB : ce fichier est la SOURCE DE VERITE injectee dans le prompt du LLM.
--      Il decrit la base reelle produite par la couche Gold (Airflow -> Postgres) ;
--      l'application ne cree plus aucune base, elle interroge celle-ci en lecture.
-- ============================================================================

-- Dimension : editions de la Coupe du Monde (une ligne par annee / pays hote).
CREATE TABLE dim_edition (
    id_edition           INTEGER PRIMARY KEY,   -- cle de substitution
    annee                INTEGER,               -- ex. 2018, 2022
    pays_hote            TEXT,                  -- ex. 'Russia', 'Qatar'
    nb_equipes           INTEGER,
    champion             TEXT,
    finaliste            TEXT,
    meilleur_buteur      TEXT,
    meilleur_buteur_buts INTEGER,
    affluence_totale     BIGINT,
    affluence_moyenne    INTEGER,
    nb_matchs            INTEGER
);

-- Dimension : equipes nationales. Le libelle du pays est en ANGLAIS
-- (ex. 'Spain', 'Brazil', 'South Korea', 'United States').
CREATE TABLE dim_equipe (
    id_equipe     INTEGER PRIMARY KEY,          -- cle de substitution
    pays          TEXT,                         -- nom du pays, en anglais
    confederation TEXT,                         -- ex. 'UEFA', 'CONMEBOL'
    code_pays     TEXT                          -- code ISO, ex. 'FRA'
);

-- Dimension : joueurs.
CREATE TABLE dim_joueur (
    id_joueur   INTEGER PRIMARY KEY,
    nom         TEXT,
    poste       TEXT,
    nationalite TEXT
);

-- Dimension : stades.
CREATE TABLE dim_stade (
    id_stade  INTEGER PRIMARY KEY,
    nom_stade TEXT,
    ville     TEXT,
    pays_hote TEXT
);

-- Fait : un match par ligne. Grain = un match.
-- dim_equipe est une DIMENSION A ROLES : chaque match la reference DEUX fois.
--   id_equipe_domicile  : equipe qui recoit  (jouant "a domicile").
--   id_equipe_exterieur : equipe qui se deplace ("a l'exterieur").
-- IMPORTANT pour toute agregation par equipe :
--   Une equipe joue tantot a domicile, tantot a l'exterieur. Pour calculer les
--   buts marques (ou encaisses, ou le nombre de matchs) d'UNE equipe, il faut
--   AGREGER SES DEUX ROLES : additionner les cas ou elle est id_equipe_domicile
--   (ses buts = score_domicile) ET les cas ou elle est id_equipe_exterieur
--   (ses buts = score_exterieur). Un simple JOIN sur un seul role sous-compte.
-- La colonne `annee` est denormalisee ici : filtrer par annee ne necessite PAS
-- de jointure vers dim_edition.
CREATE TABLE fait_match (
    id_match            TEXT PRIMARY KEY,        -- cle de substitution
    id_edition          INTEGER REFERENCES dim_edition(id_edition),
    id_equipe_domicile  INTEGER REFERENCES dim_equipe(id_equipe),
    id_equipe_exterieur INTEGER REFERENCES dim_equipe(id_equipe),
    id_stade            INTEGER REFERENCES dim_stade(id_stade),
    date_match          DATE,
    annee               INTEGER,                 -- annee de l'edition (denormalisee)
    phase               TEXT,                    -- ex. 'Group stage', 'Final'
    score_domicile      INTEGER,                 -- buts de l'equipe a domicile
    score_exterieur     INTEGER,                 -- buts de l'equipe a l'exterieur
    xg_domicile         DOUBLE PRECISION,
    xg_exterieur        DOUBLE PRECISION,
    penalty_domicile    INTEGER,                 -- buts en seance de tirs au but
    penalty_exterieur   INTEGER,
    seance_tirs_au_but  BOOLEAN,
    affluence           BIGINT
);

-- Fait : un but par ligne. Grain = un but marque. Se rattache a un match,
-- un joueur et l'equipe qui a marque.
CREATE TABLE fait_but (
    id_but            INTEGER PRIMARY KEY,
    id_match          TEXT    REFERENCES fait_match(id_match),
    id_joueur         INTEGER REFERENCES dim_joueur(id_joueur),
    id_equipe         INTEGER REFERENCES dim_equipe(id_equipe),  -- equipe du buteur
    minute            INTEGER,
    temps_additionnel INTEGER,
    minute_absolue    INTEGER,
    periode           TEXT,                      -- ex. '1re mi-temps'
    type_but          TEXT,
    est_penalty       BOOLEAN,
    est_csc           BOOLEAN                    -- but contre son camp
);

-- ---------------------------------------------------------------------------
-- Tables metier (marts) : agregats pre-calcules, prets a servir. Preferer ces
-- tables quand la question y correspond directement (plus simple et plus sur).
-- ---------------------------------------------------------------------------

-- Classement des buteurs, tous temps confondus (deja trie par nb_buts desc).
CREATE TABLE mart_classement_buteurs (
    rang        BIGINT,
    id_joueur   BIGINT,
    nom         TEXT,
    nationalite TEXT,
    nb_buts     BIGINT,
    nb_penaltys BIGINT
);

-- Statistiques par edition (une ligne par edition).
CREATE TABLE mart_stats_edition (
    id_edition             BIGINT,
    annee                  BIGINT,
    pays_hote              TEXT,
    nb_matchs              BIGINT,
    nb_buts                BIGINT,
    moyenne_buts_par_match DOUBLE PRECISION,
    affluence_moyenne      BIGINT
);

-- Statistiques cumulees par equipe (tous roles agreges, une ligne par equipe).
CREATE TABLE mart_stats_equipe (
    id_equipe       BIGINT,
    pays            TEXT,
    confederation   TEXT,
    nb_matchs       BIGINT,
    victoires       BIGINT,
    nuls            BIGINT,
    defaites        BIGINT,
    buts_marques    BIGINT,
    buts_encaisses  BIGINT,
    difference_buts BIGINT
);

-- Repartition des buts par periode de jeu.
CREATE TABLE mart_buts_par_periode (
    periode     TEXT,
    nb_buts     BIGINT,
    pourcentage DOUBLE PRECISION
);
