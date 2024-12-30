# import subprocess
import os
#from marker.convert import convert_single_pdf
#from marker.models import load_all_models
#from marker.output import save_markdown



SAVE_DIR = "output/SEC_EDGAR_FILINGS_MD"

'''
# def run_marker(input_ticker_year_path:str,ticker:str,year:str,workers:int=4,max_workers:int=8,num_chunks:int=1):
def run_marker(
    input_ticker_year_path: str, output_ticker_year_path:str,batch_multiplier: int = 2
):
    

    # subprocess.run(["marker", input_ticker_year_path,output_ticker_year_path,  "--workers", str(workers), "--num_chunks",str(num_chunks),"--max", str(max_workers) ,"--metadata_file", path_to_metadata])
    # return
    model_lst = load_all_models()
    for input_path in os.listdir(input_ticker_year_path):
        if not input_path.endswith(".pdf"):
            continue
        input_path = os.path.join(input_ticker_year_path, input_path)
        full_text, images, out_meta = convert_single_pdf(
            input_path, model_lst, langs=["English"], batch_multiplier=batch_multiplier
        )
        fname = os.path.basename(input_path)
        subfolder_path = save_markdown(
            output_ticker_year_path, fname, full_text, images, out_meta
        )
        print(f"Saved markdown to the {subfolder_path} folder")
    del model_lst

'''
import os
import time
from marker.config.parser import ConfigParser
from marker.models import create_model_dict
from marker.converters.pdf import PdfConverter
from marker.output import save_output
from marker.logger import configure_logging

# Setup logging as per the new structure
configure_logging()

def run_marker(input_ticker_year_path: str, output_ticker_year_path: str):
    # Load models and create the configuration parser
    models = create_model_dict()
    config_parser = ConfigParser()  # Assuming default configurations or modify as needed

    for input_path in os.listdir(input_ticker_year_path):
        if input_path.endswith(".pdf"):
            full_path = os.path.join(input_ticker_year_path, input_path)
            output_folder = os.path.join(output_ticker_year_path, os.path.splitext(input_path)[0])

            # Ensure the output folder exists
            os.makedirs(output_folder, exist_ok=True)
            
            # Start conversion
            start_time = time.time()
            converter = PdfConverter(
                config=config_parser.generate_config_dict(),
                artifact_dict=models,
                processor_list=config_parser.get_processors(),
                renderer=config_parser.get_renderer()
            )

            # Process each PDF
            rendered = converter.convert(full_path)
            save_output(rendered, output_folder, config_parser.get_base_filename(full_path))
            
            # Log the output and processing time
            print(f"Saved markdown to {output_folder}")
            print(f"Total time: {time.time() - start_time} seconds")

# Example usage
input_dir = "path/to/input/pdf_folder"
output_dir = "path/to/output/markdown_folder"
run_marker(input_dir, output_dir)
