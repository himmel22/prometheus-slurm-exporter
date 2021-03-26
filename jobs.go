package main

import (
	"io/ioutil"
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "slurm_job"
)

var (
	labels = []string{"user", "account", "hostname", "state", "jobid"}
)

func JobsData() []byte {
	cmd := exec.Command("scontrol", "show", "job", "-d", "-o")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

func GetHostname(node string) []string {
	cmd := exec.Command("scontrol", "show", "hostname", node)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	hostnames := strings.Split(string(out), "\n")
	return hostnames
}

type JobsMetrics struct {
	jobid    string
	user     string
	account  string
	state    string
	hostname string
	cpus     float64
	gpus     float64
	mems     float64
}

func ParseJobsMetrics(input []byte) []*JobsMetrics {
	var jobs []*JobsMetrics
	lines := strings.Split(string(input), "\n")

	re_jobid := regexp.MustCompile(`JobId=([0-9]+)`)
	re_node_string := regexp.MustCompile(`\sNodes.*?GRES.*?\s`)
	re_node := regexp.MustCompile(`\sNodes=([^\s]+)`)
	re_user := regexp.MustCompile(`UserId=([\w]+)\(`)
	re_account := regexp.MustCompile(`Account=([^\s]+)`)
	re_cpus := regexp.MustCompile(`CPU_IDs=([0-9]+)-([0-9]+)`)
	re_gpus := regexp.MustCompile(`GRES=gpu:([0-9]+)`)
	re_mems := regexp.MustCompile(`\sMem=([0-9]+)`)
	re_state := regexp.MustCompile(`JobState=([^\s]+)`)

	for _, line := range lines {
		if len(line) > 10 {
			jobid := re_jobid.FindStringSubmatch(line)[1]
			user := re_user.FindStringSubmatch(line)[1]
			account := re_account.FindStringSubmatch(line)[1]
			state := re_state.FindStringSubmatch(line)[1]
			node_strings := re_node_string.FindAllStringSubmatch(line, -1)

			for _, node_str := range node_strings {
				var (
					f_cpus float64 = 0
					f_gpus float64 = 0
					f_mems float64 = 0
				)

				cpus := re_cpus.FindStringSubmatch(node_str[0])
				if len(cpus) > 1 {
					f_cpus_start, _ := strconv.ParseFloat(cpus[1], 64)
					f_cpus_end, _ := strconv.ParseFloat(cpus[2], 64)
					f_cpus = f_cpus_end - f_cpus_start + 1
				}
				gpus := re_gpus.FindStringSubmatch(node_str[0])
				if len(gpus) > 1 {
					f_gpus, _ = strconv.ParseFloat(gpus[1], 64)
				}
				mems := re_mems.FindStringSubmatch(node_str[0])
				if len(mems) > 1 {
					f_mems, _ = strconv.ParseFloat(mems[1], 64)
				}

				nodes := re_node.FindStringSubmatch(node_str[0])
				if len(nodes) > 1 {
					if strings.Contains(nodes[1], "[") {
						hostnames := GetHostname(nodes[1])
						for _, host := range hostnames {
							if len(host) > 0 {
								jobs = append(jobs, &JobsMetrics{jobid, user, account, state, host, f_cpus, f_gpus, f_mems})
							}
						}
					} else {
						jobs = append(jobs, &JobsMetrics{jobid, user, account, state, nodes[1], f_cpus, f_gpus, f_mems})
					}
				}
			}
		}
	}
	return jobs
}

type JobsCollector struct {
	sync.Mutex
	jobCPUs *prometheus.GaugeVec
	jobGPUs *prometheus.GaugeVec
	jobMEMs *prometheus.GaugeVec
}

func NewJobsCollector() *JobsCollector {
	return &JobsCollector{
		jobCPUs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cpus",
				Help:      "Total cpu of a job",
			},
			labels,
		),
		jobGPUs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "gpus",
				Help:      "Total gpu of a job",
			},
			labels,
		),
		jobMEMs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "mems",
				Help:      "Total mem of a job",
			},
			labels,
		),
	}
}

func (jc *JobsCollector) Describe(ch chan<- *prometheus.Desc) {
	jc.jobCPUs.Describe(ch)
	jc.jobGPUs.Describe(ch)
	jc.jobMEMs.Describe(ch)
}

func (jc *JobsCollector) Collect(ch chan<- prometheus.Metric) {
	jc.Lock()
	defer jc.Unlock()

	jc.jobCPUs.Reset()
	jc.jobGPUs.Reset()
	jc.jobMEMs.Reset()

	jm := ParseJobsMetrics(JobsData())
	for j := range jm {
		jc.jobCPUs.WithLabelValues(jm[j].user, jm[j].account, jm[j].hostname, jm[j].state, jm[j].jobid).Set(float64(jm[j].cpus))
		jc.jobGPUs.WithLabelValues(jm[j].user, jm[j].account, jm[j].hostname, jm[j].state, jm[j].jobid).Set(float64(jm[j].gpus))
		jc.jobMEMs.WithLabelValues(jm[j].user, jm[j].account, jm[j].hostname, jm[j].state, jm[j].jobid).Set(float64(jm[j].mems))
	}

	jc.jobCPUs.Collect(ch)
	jc.jobGPUs.Collect(ch)
	jc.jobMEMs.Collect(ch)
}
