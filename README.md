# Deep Crawl Recommendation Engine

The Deep Crawl Recommendation Engine (DCRE) supports domain discovery by recommending sites to deep crawl.  It does this by computing analytics on the result of a broad crawl.  A broad crawl typically captures O(hundreds of thousands to millions) of sites. This number is certainly more than a human can sort through to determine sites which may be of interest to a given domain.  The DCRE system analyzes and ranks these sites.  The highest ranked sites should be considered for additions to periodic deep crawls.

# Input
The user points the DCRE system at a directory of jl files.  The jl files contain the results of a broad crawl.  Each line is required to have a key named “url” which contains the url of a page.  It is also required to have a key name “original_content” which contains the raw HTML of the captured page.  
Input can consist of one or multiple jl files which can optionally be compressed into gz format.  The python package smart_open is used to automatically determine the presence or absence of compression in each file.

# Output
Several intermediate outputs and a final ranked list are made available to the user.  The ranked list is the output which a user may find most useful, and it is the primary product which the DCRE system generates.  The ranked list is a list of domains, netlocs, which appeared in the original input dataset.  Along with the ranked list, several analytics are computer for each domain.  These include: # outlinks, # inlinks, # unique outlinks, # unique inlinks, ratio of # inlinks to # outlinks, ratio of # unique inlinks to # unique outlinks, pagerank, pagerank based on unique links.  Currently, the domains are ranked based on # outlinks.  This metric appears to be robustly correlated with the value of the given domain to a given broad crawl.  

# Additional Comments
While several metrics are computed for each domain, the # of outlinks present in the broad crawl appears to be the most correlated with the value of that domain to a broad crawl.  Other metrics, such as the pagerank tend to be high for common commercial websites like facebook and twitter.  Links to these sites often appear on many pages as a part of the “sharing” infrastructure.

# Details and intermediate products

Internally, this domain ranking is split into 3 major milestones:
  * Parse links from HTML
  * Collect/count links
  * Compute metrics
  
  **Parse Links**
  The parsing portion include three different parsing styles.  They are: Complete hyperlink (drop parameters, anchors); everything up to the first slash (e.g. https://london.craigslist.co.uk/); only the domain name and TLD (craigslist.co.uk).  The example shows a TLD that contains two ".", for example .co.uk.  Typical TLD's only contain one "." such as: .com, .net, etc.
  
 **Collect and Count** (coming soon)
 The collect and count portion imports the links parsed previously and aggragates them.
 
 **Compute Metrics** (coming soon)
 Sites of interest can be identified based on the ratio of in-links to out-links, pagerank, etc.  Metrics are computed and exported for each site.  Finally, the sites can be sorted by their metrics to yield a ranked list of sites which are then appropriate for human vetting.
 
