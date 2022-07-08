# File Update Script

This script is to be used for any deletion request. This might be suitable to run for:
- Consented tag has been removed
- Data might be corrupt
- Data should not be there in the first place

To run just fill in the information in `main.py` line 18 - line 30

After done filling the information, execute the script with:
```
python3 main.py
```

NOTE: Make sure AWS_PROFILE is set before running the script.

## The setup before executing the script

1. Clone this repository

   ```
    git clone https://github.com/umccr/agha-data-validation-pipeline.git
   ```

2. Go to this directory

    ```
    cd scripts/handle_delete_files
    ```
3. Setup an virtual environment and install packages
   ```
   python3 -mvenv .venv
   source .venv/bin/activate  # This might be different for non-unix shell
   pip install -r requirements.txt
   ```
4. Setup AWS_PROFILE.
   ```
   export AWS_PROFILE=agha
   ```

   or

   ```
   aws configure
   ```
