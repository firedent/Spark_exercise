# Spark exercise

## Problem Stateement

Given a log file that records HTTP requests (GET and POST) sent to a set of servers, you are asked to compute descriptive statistics on the amount of data (payload) communicated through such requests. These statistics need to be computed for each base URL (e.g., http://subdom0001.example.com) in the log and the results must be reported in bytes. More specifically, you need to compute the following statistics per each sub-domain):

**Minimum payload**: The smallest payload communicated for each base URL.

**Maximum payload**: The largest payload communicated for each base URL.

**Mean payload**: Mean of payloads for each base URL. For computing the mean, consider the following formula (population mean):

$$
\mu=\frac{1}{N}\sum_{i=1}^{N}x_i
$$

where N is the size of the population being explored.

**Variance of payload**: The variance of payloads for each base URL. For computing the variance, consider the following formula (population variance):

$$
\sigma^2=\frac{1}{N}\sum_{i=1}^{N}{(x_i-\mu)}^2
$$

where N is the population size and 𝜇 is the population mean.

## Input

The log file in input is a CSV file that contains one line HTTP per request. The file consists of three columns: Base URL of the HTTP request, endpoint, HTTP method, and size of payload. An excerpt of the log is shown below:

```
http://subdom0001.example.com,/endpoint0001,POST,3B
http://subdom0002.example.com,/endpoint0002,GET,431MB
http://subdom0003.example.com,/endpoint0003,POST,231KB
http://subdom0002.example.com,/endpoint0002,GET,29MB
http://subdom0001.example.com,/endpoint0001,POST,238B
http://subdom0002.example.com,/endpoint0001,GET,32MB
http://subdom0003.example.com,/endpoint0003,GET,21KB
```

Notice that the payload is given in different units of digital information (e.g. MB and KB). The log file for this assignment will contain the following units only: B (for bytes), KB (for kilobytes) and MB (for megabytes).

## Output

The output consists in a CSV file that contains the list of base URLs along with the descriptive statistics (for each base URL) as presented in Section 1. We show a sample output below:

```
http://subdom0001.example.com,3B,238B,120B,13806B
http://subdom0002.example.com,30408704B,451936256B,171966464B,39193191483703296B
http://subdom0003.example.com,21504B,236544B,129024B,11560550400B
```

The columns in the output above are as follows: Base URL, minimum payload, maximum payload, mean payload, variance of payload. You do not need to provide a header for your CSV file.

Notice that the statistics must be expressed in bytes (B), as shown in the example above. Note also that you may get float / double numbers when computing means and variance. In such cases, truncate the numbers to keep just the whole number part. For example:
* 120938.32 -> 120938
* 9983.89 -> 9983

## Solution

This scala program is working for a count payload size of each base URL via RDDs in Spark framework.

1. Read the file in the function of SparkContext. The goal of this is converting text file to RDD object.
2. Before convert, each line of the file to key-value pair, use filter to remove the empty line from records. After filtering, use the map function to apply the customized function to each line for produce key-value pair (BaseURL, Payload size).
3. Use 'groupByKey' to group the set of key-value pair by key for producing a sorted array containing payload size
4. Calculate mean, variance, minimum and maximum via the two functions named 'mean' and 'variance'.



