Resume Parser Pipeline (Airflow + Llama3 + S3 + Python)                                                                                                                  
--------------------------------------------------------------                                                                                                       
This project automates parsing resumes stored in an S3 bucket (LocalStack),                                                                                                      
extracts text, analyzes it using Llama3, and returns structured JSON with                                                                                                  
candidate name, email, predicted job role, and suitability score.                                                                                                                
                                                                                                                              
Airflow runs the pipeline hourly, grabs every PDF from the S3 bucket,                                                                                        
processes it, and logs the LLM output.                                                                                                  

Airflow                                                                                                                           
                                                                                                                                            
<img width="1071" height="520" alt="image" src="https://github.com/user-attachments/assets/b2330dc4-7eed-4a3c-adda-458724948636" />



