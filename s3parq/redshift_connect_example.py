
# # When building the cluster, make sure you set the role of the cluster
# # to have access to S3


# create external schema spectrum 
# from data catalog 
# database 'spectrumdb' 
# iam_role 'arn:aws:iam::265991248033:role/mySpectrumRole'
# create external database if not exists;

# create external table spectrum.test11 (shoelace varchar)
# partitioned by (hamburger varchar, hot_dog int)
# stored as parquet 
# location 's3://ichain-dev/dc51-contract-spectrum-support/derp/hamberder/';


# alter table spectrum.test11
# add partition (hamburger='abnvoejvaspabwgbjinwcr', hot_dog=926099)
# location 's3://ichain-dev/dc51-contract-spectrum-support/derp/hamberder/hamburger=abnvoejvaspabwgbjinwcr';


# select * from spectrum.test11

# SQLAlchemy will treat redshift table same way as a postgres table
