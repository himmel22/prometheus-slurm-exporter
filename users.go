/* Copyright 2020 Victor Penso

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
        "io/ioutil"
        "os/exec"
        "log"
        "strings"
        "strconv"
        "regexp"
        "github.com/prometheus/client_golang/prometheus"
)

func UsersData() []byte {
        cmd := exec.Command("squeue","-a","-r","-h","-O","jobid,username,state,tres:50,nodelist")
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

type UserJobMetrics struct {
        pending float64
        running float64
        running_cpus float64
        running_mem float64
        running_gpus float64
        suspended float64
}

func ParseUsersMetrics(input []byte) map[string]*UserJobMetrics {
        users := make(map[string]*UserJobMetrics)
        lines := strings.Split(string(input), "\n")
        for _, line := range lines {
                line_fields := strings.Fields(line)
                if len(line_fields) > 3 {
                        user := line_fields[1]
                        _,key := users[user]
                        if !key {
                                users[user] = &UserJobMetrics{0,0,0,0,0,0}
                        }
                        state := line_fields[2]
                        state = strings.ToLower(state)
                        tres_string := line_fields[3]
                        re_cpu := regexp.MustCompile("cpu=([0-9]+)")
                        re_gpu := regexp.MustCompile("gpu=([0-9]+)")
                        re_mem := regexp.MustCompile("mem=([0-9]+[A-Z])")
                        cpu_string := re_cpu.FindString(tres_string)
                        gpu_string := re_gpu.FindString(tres_string)
                        mem_string := re_mem.FindString(tres_string)
                        re_number := regexp.MustCompile("[0-9]+")
                        cpus,_ := strconv.ParseFloat(re_number.FindString(cpu_string),64)
                        gpus,_ := strconv.ParseFloat(re_number.FindString(gpu_string),64)
                        mem,_ := strconv.ParseFloat(re_number.FindString(mem_string),64)
                        if strings.Contains(mem_string, "M") {
                                mem /= 1000
                        } 
                        pending := regexp.MustCompile(`^pending`)
                        running := regexp.MustCompile(`^running`)
                        suspended := regexp.MustCompile(`^suspended`)
                        switch {
                        case pending.MatchString(state) == true:
                                users[user].pending++
                        case running.MatchString(state) == true:
                                users[user].running++
                                users[user].running_cpus += cpus
                                users[user].running_gpus += gpus
                                users[user].running_mem += mem
                        case suspended.MatchString(state) == true:
                                users[user].suspended++
                        }
                }
        }
        return users
}

type UsersCollector struct {
        pending *prometheus.Desc
        running *prometheus.Desc
        running_cpus *prometheus.Desc
        running_gpus *prometheus.Desc
        running_mem *prometheus.Desc
        suspended *prometheus.Desc
}

func NewUsersCollector() *UsersCollector {
        labels := []string{"user"}
        return &UsersCollector {
                pending: prometheus.NewDesc("slurm_user_jobs_pending", "Pending jobs for user", labels, nil), 
                running: prometheus.NewDesc("slurm_user_jobs_running", "Running jobs for user", labels, nil),
                running_cpus: prometheus.NewDesc("slurm_user_cpus_running", "Running cpus for user", labels, nil),
                running_gpus: prometheus.NewDesc("slurm_user_gpus_running", "Running gpus for user", labels, nil),
                running_mem: prometheus.NewDesc("slurm_user_mem_running", "Running mem for user", labels, nil),
                suspended: prometheus.NewDesc("slurm_user_jobs_suspended", "Suspended jobs for user", labels, nil),
        }
}

func (uc *UsersCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- uc.pending
        ch <- uc.running
        ch <- uc.running_cpus
        ch <- uc.running_gpus
        ch <- uc.running_mem
        ch <- uc.suspended
}

func (uc *UsersCollector) Collect(ch chan<- prometheus.Metric) {
        um := ParseUsersMetrics(UsersData())
        for u := range um {
                if um[u].pending > 0 {
                        ch <- prometheus.MustNewConstMetric(uc.pending, prometheus.GaugeValue, um[u].pending, u)
                }
                if um[u].running > 0 {
                        ch <- prometheus.MustNewConstMetric(uc.running, prometheus.GaugeValue, um[u].running, u)
                }
                if um[u].running_cpus > 0 {
                        ch <- prometheus.MustNewConstMetric(uc.running_cpus, prometheus.GaugeValue, um[u].running_cpus, u)
                }
                if um[u].running_gpus > 0 {
                        ch <- prometheus.MustNewConstMetric(uc.running_gpus, prometheus.GaugeValue, um[u].running_gpus, u)
                }
                if um[u].running_mem > 0 {
                        ch <- prometheus.MustNewConstMetric(uc.running_mem, prometheus.GaugeValue, um[u].running_mem, u)
                }
                if um[u].suspended > 0 {
                        ch <- prometheus.MustNewConstMetric(uc.suspended, prometheus.GaugeValue, um[u].suspended, u)
                }
        }
}

