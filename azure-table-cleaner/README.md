# Azure Table Cleaner

This project is an Azure Function App written in Python for cleaning up Azure Table Storage. It is designed to automate the process of removing old or unnecessary records from your Azure tables.

## Features
- Cleans Azure Table Storage based on custom logic
- Configurable via `local.settings.json`
- Easy deployment to Azure using Azure Functions

## Prerequisites
- Python 3.8+
- Azure Functions Core Tools
- An Azure subscription

## Getting Started

### 1. Clone the repository
```sh
git clone https://github.com/yourusername/azure-table-cleaner.git
cd azure-table-cleaner
```

### 2. Install dependencies
```sh
pip install -r requirements.txt
```

### 3. Configure settings
Edit `local.settings.json` to add your Azure Storage connection string and any other required settings.

### 4. Run locally
```sh
func start
```

### 5. Deploy to Azure
```sh
func azure functionapp publish <YourFunctionAppName> --python
```

## File Structure
- `function_app.py`: Main function app code
- `host.json`: Azure Functions host configuration
- `local.settings.json`: Local settings for development
- `requirements.txt`: Python dependencies

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
MIT License
