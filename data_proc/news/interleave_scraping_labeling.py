"""Run scraping and labeling in an interleaved fashion"""
from data_proc.news.labeling import EXTRACT_EVENTS_TMPL, GROQ_DEFAULT_MODEL, run_labeling
from data_proc.news.scraping import run_scraping


def main() -> None:
    """"Run scraping and labeling in an interleaved manner"""
    while True:
        run_scraping(batch_size=10, limit=1000)
        run_labeling(prompt_tmpl=EXTRACT_EVENTS_TMPL,
                     model=GROQ_DEFAULT_MODEL)

if __name__ == "__main__":
    main()
