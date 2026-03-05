This project utilizes the official NBA_API to retrieve data regarding professional basketball players, then convert that data into SQL DB's. Through Langchain and OpenAI, we designed a RAG architecture that generates an accurate SQL query and format that data into an answer to user queries. The project has online capabilities through streamlit, and has a backend that relies on Python.

This project was inspired by StatMuse.com, which utilizes similar technologies to generate answers to basketball-related queries

Before running, make sure to add an .env file with 

```OPENAI_API_KEY = <api-key>```

and 

```LANGCHAIN_API_KEY= <api-key>```

Also, run 

```pip install -r requirements.txt```

in terminal to install the necessary libraries

To run app, use 

```python -m streamlit run app.py```

in the frontend folder

