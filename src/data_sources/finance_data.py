from src.data_sources.earnings_calls_src import get_earnings_all_docs
from src.data_sources.filings_src import sec_main as unstructured_sec_main
from src.data_sources.marker_sec_src.sec_filings_to_pdf import sec_save_pdfs
from src.data_sources.marker_sec_src.pdf_to_md import run_marker as run_marker_single
from src.data_sources.marker_sec_src.pdf_to_md_parallel import run_marker_mp
from typing import List, Optional
import os

SAVE_DIR = "output/SEC_EDGAR_FILINGS_MD"

def get_data(
        ticker:str,
        year:str,
        filing_types:List[str] = ["10-K", "10-Q"],
        data_source:str = 'unstructured',
        include_amends=True,
        batch_processing:bool=False,
        batch_multiplier:Optional[int]=None,
        workers:Optional[int] = None,
        inference_ram:Optional[int] = None,
        vram_per_task:Optional[int] = None,
        num_chunks:int = 1,
):
    assert data_source in ['unstructured','earnings_calls','marker_pdf'], "The valid data sources are ['unstructured','earnings_calls','marker_pdf']"
    
    if 'marker_pdf' in data_source:
        # subprocess.run(["ls", "-l"])
        os.makedirs(SAVE_DIR, exist_ok=True)
        # path_to_metadata = os.path.join(input_ticker_year_path,"metadata.json")
        output_ticker_year_path = os.path.join(SAVE_DIR, f"{ticker}-{year}")
        os.makedirs(output_ticker_year_path, exist_ok=True)
        html_urls, metadata_json, metadata_file_path,input_ticker_year_path = sec_save_pdfs(
            ticker, year, filing_types, include_amends
        )
        if not batch_processing:
            assert batch_multiplier is not None, "The batch multiplier is not specified"
            run_marker_single(
            input_ticker_year_path=input_ticker_year_path,
            output_ticker_year_path=output_ticker_year_path,
            batch_multiplier=batch_multiplier,
        )
        elif batch_processing:
            run_marker_mp(
                in_folder=input_ticker_year_path,
                out_folder=output_ticker_year_path,
                metadata_file=metadata_file_path,
                inference_ram=inference_ram,
                workers=workers,
                num_chunks=num_chunks,
                vram_per_task=vram_per_task,
            )
        print(f"Files have been saved successfully. Check in the folder {output_ticker_year_path}")
    elif 'unstructured' in data_source:
        sec_data,sec_form_names = unstructured_sec_main(ticker,year,filing_types,include_amends)
        return sec_data,sec_form_names
    elif 'earnings_calls' in data_source:
        earnings_docs,earnings_call_quarter_vals,speakers_list_1,speakers_list_2,speakers_list_3,speakers_list_4 = get_earnings_all_docs(ticker,year)
        return (
            earnings_docs,
            earnings_call_quarter_vals,
            speakers_list_1,
            speakers_list_2,
            speakers_list_3,
            speakers_list_4,
        )

if __name__ == "__main__":
    ticker = "AAPL"
    year = "2023"
    filing_types = ["10-K", "10-Q"]

    earnings_docs, earnings_call_quarter_vals, speakers_list_1, speakers_list_2, speakers_list_3, speakers_list_4 = get_data(ticker=ticker,year=year,data_source='earnings_calls')
    #sec_data,sec_form_names = get_data(ticker=ticker, year=year,data_source='unstructured',include_amends=True,filing_types=filing_types)
    #sec_data,sec_form_names = get_data(ticker=ticker, year=year,data_source='unstructured',include_amends=True,filing_types=filing_types)
    #get_data(ticker=ticker,year=year,data_source='marker_pdf',batch_processing=False,batch_multiplier=1)