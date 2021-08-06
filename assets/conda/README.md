Build on AWS EC2 instance (see github.com/scwatts/ec2\_work\_instance); on remote write `meta.yaml` and `build.sh` to `/conda/meta.yaml` and then build
```bash
conda-init
conda install conda-build
conda build -c bioconda -c conda-forge -c defaults conda/meta.yaml
```

Rather than having to install anaconda on the instance, I download the built package and upload from local
```bash
# Grab package location from console output of above build command
# Upload to S3 from the instance
aws s3 cp /data/miniconda3/conda-bld/linux-64/fqtools-2.3-h9bf148f_1.tar.bz2 s3://umccr-temp-dev/stephen/

# Now *locally* download and send to conda repo
aws s3 cp s3://umccr-temp-dev/stephen/fqtools-2.3-h9bf148f_1.tar.bz2 .
/usr/local/anaconda3/bin/anaconda login
/usr/local/anaconda3/bin/anaconda upload fqtools-2.3-h9bf148f_1.tar.bz2
```
