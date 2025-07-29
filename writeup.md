# Filtering Language Modeling Data

## Filtering Common Crawl (CC):

### Inspecting CC:

I downloaded sample files as WARC and WET to get an idea of what a random 
sample would contain.

1. The WARC file contains URLs, metadata, HTTP request details and the 
raw HTML content.
2. The WET file contains only the extracted text parts. However, it's every single text in the page
not just the main content, looking at the WET file all the headers, footers, and buttons should have been 
filtered by the extractor.
3. Looking at more than 25 WET records, it is clear that "high quality" webpages 
are very rare, none of the ones I saw is something I thought LMs were or should be trained on.
it felt like complete gibberish.

Looking at the WET files, I think I can do a better job in HTML to text conversion.

### HTML to text conversion:
