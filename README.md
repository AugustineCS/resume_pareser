Resume Parser Pipeline (Airflow + Llama3 + S3 + Python)                                                                                                                  
--------------------------------------------------------------                                                                                                       
This project automates parsing resumes stored in an S3 bucket (LocalStack),                                                                                                      
extracts text, analyzes it using Llama3, and returns structured JSON with                                                                                                  
candidate name, email, predicted job role, and suitability score.                                                                                                                
                                                                                                                              
Airflow runs the pipeline hourly, grabs every PDF from the S3 bucket,                                                                                        
processes it, and logs the LLM output.                                                                                                  



