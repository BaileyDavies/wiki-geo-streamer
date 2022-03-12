# WIKI GEO STREAMER
Wiki Geo Streamer is a Python application that connects to a Wikipedia socket that sends data related to page updates. This application gathers this data and sends verified Geo location and related fields to a connected Elasticsearch instance for processing. 

# Running This Application

To  run this application please configure your elasticsearch variables related to you AWS access keys through the related enviroment variables. Please run the unit and integration tests in the Test folder before any new deployments
# Tools

1. The Language being used is Python
2. The deployment method being utilised is AWS Lambda, but you can run this project locally if you want.
3. The integrations being used are: Elasticsearch