#!/bin/sh

# DBSERVER='localhost'
# DBPORT='5432'
# DBUSERNAME='postgres'
# DBPASSWORD='abcde'

NOW=$(date +"%F")
LOGFILE="Logs/etl-log-$NOW.log"

wget --directory-prefix=./Data/ https://datasets.imdbws.com/name.basics.tsv.gz >> $LOGFILE
wget --directory-prefix=./Data/ https://datasets.imdbws.com/title.akas.tsv.gz >> $LOGFILE
wget --directory-prefix=./Data/ https://datasets.imdbws.com/title.basics.tsv.gz >> $LOGFILE
wget --directory-prefix=./Data/ https://datasets.imdbws.com/title.crew.tsv.gz >> $LOGFILE
wget --directory-prefix=./Data/ https://datasets.imdbws.com/title.episode.tsv.gz >> $LOGFILE
wget --directory-prefix=./Data/ https://datasets.imdbws.com/title.principals.tsv.gz >> $LOGFILE
wget --directory-prefix=./Data/ https://datasets.imdbws.com/title.ratings.tsv.gz >> $LOGFILE

COUNT=$(ls -1U Data | wc -l)

if [ $COUNT -eq 7 ]
then
    { 
        s32imdbpy.py ./Data/ postgresql://$DBUSERNAME:$DBPASSWORD@$DBSERVER:$DBPORT/imdb >> $LOGFILE
    } || { 
        echo "Data upload incomplete at `date +"%d-%m-%Y-%H-%M-%S"`." >> $LOGFILE
    }
else
   echo "Files dowload incomplete at `date +"%d-%m-%Y-%H-%M-%S"`." >> $LOGFILE
fi
