# RAG SQL Reader Application

This project is a Retrieval-Augmented Generation (RAG) application designed to streamline interactions with a SQL 
database. By harnessing the power of Function Calling, the application adapts 
a Large Language Model (LLM) to the specific database structure. Gemini-Pro, a language model from Google, is utilized for this purpose. The front end of the application is developed using Gradio, providing an intuitive interface for users to seamlessly interact with the database.

# Features

- SQL Query Generation: The application is capable of generating SQL queries based on user input. Users can input their 
queries or questions, and the application utilizes the underlying LLM to generate corresponding SQL queries tailored
to the database schema.

- Chatbot Interface: The front end of the application is developed using Gradio, a user-friendly framework for 
building interactive web applications. The Gradio interface provides users with an intuitive and responsive chatbot
platform to interact with the database, facilitating seamless query formulation and data exploration.

# Installation
Create a new virtual environment using Conda:

    conda create -n CBS_Statistical_Assistent python=3.12.8

Activate the environment:

    conda activate CBS_Statistical_Assistent

Install the required Python libraries:

    pip install -r requirements.txt`

# Usage 
Create a `.env` file
In the `.env` file, insert your [Google API Key](https://aistudio.google.com/app/apikey):

    GOOGLE_API_KEY=your_google_api_key

Activate the Conda environment:

    conda activate CBS_Statistical_Assistent

From the root directory, run the following command:

    python src/main.py