# AWS AGHA-GDR S3 DATA SHARING

This is a step-by-step guide for AGHA-GDR data sharing via [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/GetStartedWithS3.html) (Cloud storage service).

- [AWS AGHA-GDR S3 DATA SHARING](#aws-agha-gdr-s3-data-sharing)
  - [Step 1 - Do you have an AWS account?](#step-1---do-you-have-an-aws-account)
  - [Step 2 - Prepare an S3 bucket](#step-2---prepare-an-s3-bucket)
    - [Creating the S3 bucket](#creating-the-s3-bucket)
    - [Getting the S3 ARN (Amazon Resource Number)](#getting-the-s3-arn-amazon-resource-number)
  - [Step 3 - Permission for the AGHA AWS account to send data to your s3 bucket](#step-3---permission-for-the-agha-aws-account-to-send-data-to-your-s3-bucket)
    - [Creating Policy on S3 bucket](#creating-policy-on-s3-bucket)
  - [Step 4 - Let the AGHA-GDR administrator know](#step-4---let-the-agha-gdr-administrator-know)
  - [Notes](#notes)

The AGHA-GDR data is currently available on AWS S3. If we need to share this data outside the AWS environment, there will be an egress cost as data will be transferred to the public internet. We can avoid this cost if we transfer data within the AWS environment. If you decide to view or process data within AWS, you will not have any egress costs unless you transfer data between AWS regions.

## Step 1 - Do you have an AWS account?
[Yes, I have. Skipping ...](#step-2---prepare-an-s3-bucket)

In order to transfer data to your S3 bucket, you must have an AWS account - have your **credit card** details ready as they are needed for the setup!

This is the official guide from AWS:
https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/

In summary:

1. Go to <https://aws.amazon.com/>.
2. Click on the `Create an AWS Account` button at the top right.
   <br/><img src="./screenshot/sign-in-button.png" width="300px"/><br/>
3. Fill in the form with the information needed (for support plan, you could choose `Basic support - Free`).
   <br/><img src="./screenshot/sign-up-page.png" width="300px"/> <img src="./screenshot/support-plan-page.png" width="300px"/><br/>
4. You may need to verify your account (check your email). After you have verified the email, go to your AWS console.
   <br/><img src="./screenshot/sign-up-complete.png" width="300px"/><br/>
   You could click on the `Go to the AWS Management Console` button or head back to https://aws.amazon.com/ and click on the `Sign In` button.
5. Select `Root user`, and fill in the email and password to log in as a root user.
   <br/><img src="./screenshot/sign-in-page.png" width="300px"/><br/>
6. Your AWS account is created! You are now logged in as a root user (user with **full** access).

**NOTE**:
In AWS, you need to have permissions to create/use/modify services (including S3 buckets).
If you are not logged in a root account, you will need full access to the S3 service from your administrator / `Root user` to setup the S3 bucket.
Please refer to IAM policy for S3 managed service for details:
https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-iam-awsmanpol.html

If you are in the root account, you do not need to worry about this.

## Step 2 - Prepare an S3 bucket

**TL;DR**:
Prepare an `ap-southeast-2` S3 bucket that will store the data.

---

The AWS documentation describes how to create an S3 bucket. Have a read through and familiarise yourself with the service:
https://docs.aws.amazon.com/AmazonS3/latest/userguide/GetStartedWithS3.html


### Creating the S3 bucket

1. Search on S3 in the search bar, and open the S3 service console.
2. Click on the `Create bucket` button.
3. Enter a Bucket name that is globally unique (yes, this means you have to come up with a name that is unique amongst all the AWS S3 buckets available in the world).
   There are some bucket naming rules that need to be followed (see them [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)).
4. **Critical**: under AWS Region, select `Asia Pacific (Sydney) ap-southeast-2`. Do **NOT** miss this step.
5. Leave the rest of the options as default and click the `Create bucket` button.
6. Your bucket is created!

### Getting the S3 ARN (Amazon Resource Number)
In AWS, every resource is labeled with an ID called Amazon Resource Number. We need this so that we can refer to this resource in the future.

1. Click on the S3 bucket with the name you created on top (could use search bucket name if needed).
2. Click on the `Properties` tab, and you should be able to copy the bucket's ARN value.
  <br/><img src="./screenshot/s3-arn.png" width="300px"/><br/>

## Step 3 - Permission for the AGHA AWS account to send data to your s3 bucket

**TL;DR**:
Allow AGHA-Service account to upload to S3.

AWS AGHA account number: `602836945884`

---

Before we can send you the data requested to your S3 bucket, you need to give us access to do so.

### Creating Policy on S3 bucket
We need to specify which actions are allowed under the bucket permissions.
1. Click on the S3 bucket with the name you created above (could use search bucket name if needed).
2. Click on the `Permissions` tab, scroll to the `Bucket policy` section, and click on the `Edit` button.
  <br/><img src="./screenshot/s3-policy.png" width="500px"/><br/>
3. Copy the following JSON or append it with the following policy. Change the `REPLACE_HERE` below (for `Resource`) with the bucket ARN created above (Refer to [Step 2](#getting-the-s3-arn-amazon-resource-number) on how to get the bucket ARN). Note that the second `Resource` value has a `/*` suffix.
    ```json
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
              "AWS": "602836945884"
          },
          "Action": [
              "s3:GetObject",
              "s3:PutObject",
              "s3:DeleteObject",
              "s3:PutObjectAcl",
              "s3:PutObjectTagging",
              "s3:ListBucket",
              "s3:GetBucketLocation"
          ],
          "Resource": [
            "REPLACE_HERE",
            "REPLACE_HERE/*"
          ]
      }
      ]
    }
    ```
Explanation of the JSON policy:
This policy will tell the bucket (specified as the ARN in the `Resource` payload) to allow the AGHA's AWS account (specified as the AGHA's account number in the `Principal` payload) to do the following actions:
-  Get/Put Object to the bucket.
-  List objects in your bucket, we wanted to make sure all data has been transferred successfully to your bucket.
-  Get your bucket location, we need to check if your bucket is in the `Asia Pacific (Sydney) ap-southeast-2` region.

## Step 4 - Let the AGHA-GDR administrator know

Your account is ready for the files to be transferred.
Send us your bucket ARN, and we will be able to copy files over to your bucket.

## Notes

- When we copy the data to your S3 bucket, the data belongs to you, and you will be charged for S3 storage. You could try to save costs if you change S3 classes. See: [S3 classes](https://aws.amazon.com/s3/storage-classes/), [S3 pricing](https://aws.amazon.com/s3/pricing/)
- Data transferred within AWS in the same region (`ap-southeast-2`) will incur no fee e.g. if you access the file via AWS EC2 (Virtual Machine). But if you decide to download this file somewhere else, it will attract egress costs.
