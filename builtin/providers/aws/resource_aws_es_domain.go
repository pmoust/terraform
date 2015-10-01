package aws

import (
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	elasticsearch "github.com/aws/aws-sdk-go/service/elasticsearchservice"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourceAwsElasticSearchDomain() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsElasticSearchDomainCreate,
		Read:   resourceAwsElasticSearchDomainRead,
		Update: resourceAwsElasticSearchDomainUpdate,
		Delete: resourceAwsElasticSearchDomainDelete,

		Schema: map[string]*schema.Schema{
			"access_policies": &schema.Schema{
				Type: schema.TypeString,
				// TODO: Proper IAM normalize function
				// See https://github.com/hashicorp/terraform/pull/3124
				StateFunc: normalizeJson,
				Optional:  true,
			},
			"advanced_options": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
				Computed: true,
			},
			"domain_name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: func(v interface{}, k string) (ws []string, errors []error) {
					value := v.(string)
					if !regexp.MustCompile(`^[0-9A-Za-z]+`).MatchString(value) {
						errors = append(errors, fmt.Errorf(
							"%q must start with a letter or number", k))
					}
					if !regexp.MustCompile(`^[0-9A-Za-z][0-9a-z-]+$`).MatchString(value) {
						errors = append(errors, fmt.Errorf(
							"%q can only contain lowercase characters, numbers and hyphens", k))
					}
					return
				},
			},
			"arn": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"domain_id": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"endpoint": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"ebs_options": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"ebs_enabled": &schema.Schema{
							Type:     schema.TypeBool,
							Required: true,
						},
						"iops": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"volume_size": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"volume_type": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
			"cluster_config": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"dedicated_master_count": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"dedicated_master_enabled": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"dedicated_master_type": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"instance_count": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
							Default:  1,
						},
						"instance_type": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
							Default:  "m3.medium.elasticsearch",
						},
						"zone_awareness_enabled": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
						},
					},
				},
			},
			"snapshot_options": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"automated_snapshot_start_hour": &schema.Schema{
							Type:     schema.TypeInt,
							Required: true,
						},
					},
				},
			},
		},
	}
}

func resourceAwsElasticSearchDomainCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).esconn

	input := elasticsearch.CreateElasticsearchDomainInput{
		DomainName: aws.String(d.Get("domain_name").(string)),
	}

	if v, ok := d.GetOk("access_policies"); ok {
		input.AccessPolicies = aws.String(v.(string))
	}

	if v, ok := d.GetOk("advanced_options"); ok {
		input.AdvancedOptions = stringMapToPointers(v.(map[string]interface{}))
	}

	if v, ok := d.GetOk("ebs_options"); ok {
		options := v.([]interface{})

		if len(options) > 1 {
			return fmt.Errorf("Only a single ebs_options block is expected")
		} else if len(options) == 1 {
			if options[0] == nil {
				return fmt.Errorf("At least one field is expected inside ebs_options")
			}

			s := options[0].(map[string]interface{})
			input.EBSOptions = expandESEBSOptions(s)
		}
	}

	if v, ok := d.GetOk("cluster_config"); ok {
		config := v.([]interface{})

		if len(config) > 1 {
			return fmt.Errorf("Only a single cluster_config block is expected")
		} else if len(config) == 1 {
			if config[0] == nil {
				return fmt.Errorf("At least one field is expected inside cluster_config")
			}
			m := config[0].(map[string]interface{})
			input.ElasticsearchClusterConfig = expandESClusterConfig(m)
		}
	}

	if v, ok := d.GetOk("snapshot_options"); ok {
		options := v.([]interface{})

		if len(options) > 1 {
			return fmt.Errorf("Only a single snapshot_options block is expected")
		} else if len(options) == 1 {
			if options[0] == nil {
				return fmt.Errorf("At least one field is expected inside snapshot_options")
			}

			o := options[0].(map[string]interface{})

			snapshotOptions := elasticsearch.SnapshotOptions{
				AutomatedSnapshotStartHour: aws.Int64(int64(o["automated_snapshot_start_hour"].(int))),
			}

			input.SnapshotOptions = &snapshotOptions
		}
	}

	log.Printf("[DEBUG] Creating ElasticSearch domain: %s", input)
	out, err := conn.CreateElasticsearchDomain(&input)
	if err != nil {
		return err
	}

	d.SetId(*out.DomainStatus.ARN)

	log.Printf("[DEBUG] Waiting for ElasticSearch domain %q to be created", d.Id())
	err = resource.Retry(15*time.Minute, func() error {
		out, err := conn.DescribeElasticsearchDomain(&elasticsearch.DescribeElasticsearchDomainInput{
			DomainName: aws.String(d.Get("domain_name").(string)),
		})
		if err != nil {
			return resource.RetryError{Err: err}
		}

		if !*out.DomainStatus.Processing && out.DomainStatus.Endpoint != nil {
			return nil
		}

		return fmt.Errorf("%q: Timeout while waiting for the domain to be created", d.Id())
	})
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] ElasticSearch domain %q created", d.Id())

	return resourceAwsElasticSearchDomainRead(d, meta)
}

func resourceAwsElasticSearchDomainRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).esconn

	out, err := conn.DescribeElasticsearchDomain(&elasticsearch.DescribeElasticsearchDomainInput{
		DomainName: aws.String(d.Get("domain_name").(string)),
	})
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] Received ElasticSearch domain: %s", out)

	ds := out.DomainStatus

	d.Set("access_policies", *ds.AccessPolicies)
	d.Set("advanced_options", pointersMapToStringList(ds.AdvancedOptions))
	d.Set("domain_id", *ds.DomainId)
	d.Set("domain_name", *ds.DomainName)
	if ds.Endpoint != nil {
		d.Set("endpoint", *ds.Endpoint)
	}

	d.Set("ebs_options", flattenESEBSOptions(ds.EBSOptions))
	d.Set("cluster_config", flattenESClusterConfig(ds.ElasticsearchClusterConfig))
	if ds.SnapshotOptions != nil {
		d.Set("snapshot_options", map[string]interface{}{
			"automated_snapshot_start_hour": *ds.SnapshotOptions.AutomatedSnapshotStartHour,
		})
	}

	d.Set("arn", *ds.ARN)

	return nil
}

func resourceAwsElasticSearchDomainUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).esconn

	input := elasticsearch.UpdateElasticsearchDomainConfigInput{
		DomainName: aws.String(d.Get("domain_name").(string)),
	}

	if d.HasChange("access_policies") {
		input.AccessPolicies = aws.String(d.Get("access_policies").(string))
	}

	if d.HasChange("advanced_options") {
		input.AdvancedOptions = stringMapToPointers(d.Get("advanced_options").(map[string]interface{}))
	}

	if d.HasChange("ebs_options") {
		options := d.Get("ebs_options").([]interface{})

		if len(options) > 1 {
			return fmt.Errorf("Only a single ebs_options block is expected")
		} else if len(options) == 1 {
			s := options[0].(map[string]interface{})
			input.EBSOptions = expandESEBSOptions(s)
		}
	}

	if d.HasChange("cluster_config") {
		config := d.Get("cluster_config").([]interface{})

		if len(config) > 1 {
			return fmt.Errorf("Only a single cluster_config block is expected")
		} else if len(config) == 1 {
			m := config[0].(map[string]interface{})
			input.ElasticsearchClusterConfig = expandESClusterConfig(m)
		}
	}

	if d.HasChange("snapshot_options") {
		options := d.Get("snapshot_options").([]interface{})

		if len(options) > 1 {
			return fmt.Errorf("Only a single snapshot_options block is expected")
		} else if len(options) == 1 {
			o := options[0].(map[string]interface{})

			snapshotOptions := elasticsearch.SnapshotOptions{
				AutomatedSnapshotStartHour: aws.Int64(int64(o["automated_snapshot_start_hour"].(int))),
			}

			input.SnapshotOptions = &snapshotOptions
		}
	}

	_, err := conn.UpdateElasticsearchDomainConfig(&input)
	if err != nil {
		return err
	}

	err = resource.Retry(25*time.Minute, func() error {
		out, err := conn.DescribeElasticsearchDomain(&elasticsearch.DescribeElasticsearchDomainInput{
			DomainName: aws.String(d.Get("domain_name").(string)),
		})
		if err != nil {
			return resource.RetryError{Err: err}
		}

		if *out.DomainStatus.Processing == false {
			return nil
		}

		return fmt.Errorf("%q: Timeout while waiting for changes to be processed", d.Id())
	})
	if err != nil {
		return err
	}

	return resourceAwsElasticSearchDomainRead(d, meta)
}

func resourceAwsElasticSearchDomainDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).esconn

	log.Printf("[DEBUG] Deleting ElasticSearch domain: %q", d.Get("domain_name").(string))
	_, err := conn.DeleteElasticsearchDomain(&elasticsearch.DeleteElasticsearchDomainInput{
		DomainName: aws.String(d.Get("domain_name").(string)),
	})
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] Waiting for ElasticSearch domain %q to be deleted", d.Get("domain_name").(string))
	err = resource.Retry(15*time.Minute, func() error {
		out, err := conn.DescribeElasticsearchDomain(&elasticsearch.DescribeElasticsearchDomainInput{
			DomainName: aws.String(d.Get("domain_name").(string)),
		})

		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok {
				return resource.RetryError{Err: err}
			}

			if awsErr.Code() == "ResourceNotFoundException" {
				return nil
			}

			return resource.RetryError{Err: awsErr}
		}

		if !*out.DomainStatus.Processing {
			return nil
		}

		return fmt.Errorf("%q: Timeout while waiting for the domain to be deleted", d.Id())
	})

	d.SetId("")

	return err
}

func pointersMapToStringList(pointers map[string]*string) map[string]interface{} {
	list := make(map[string]interface{}, len(pointers))
	for i, v := range pointers {
		list[i] = *v
	}
	return list
}

func stringMapToPointers(m map[string]interface{}) map[string]*string {
	list := make(map[string]*string, len(m))
	for i, v := range m {
		list[i] = aws.String(v.(string))
	}
	return list
}

func expandESClusterConfig(m map[string]interface{}) *elasticsearch.ElasticsearchClusterConfig {
	config := elasticsearch.ElasticsearchClusterConfig{}

	log.Printf("[DEBUG] Expanding ES Cluster Config: %#v", m)

	if v, ok := m["dedicated_master_enabled"]; ok {
		isEnabled := v.(bool)
		config.DedicatedMasterEnabled = aws.Bool(isEnabled)

		if isEnabled {
			if v, ok := m["dedicated_master_count"]; ok && v.(int) > 0 {
				config.DedicatedMasterCount = aws.Int64(int64(v.(int)))
			}
			if v, ok := m["dedicated_master_type"]; ok && v.(string) != "" {
				config.DedicatedMasterType = aws.String(v.(string))
			}
		}
	}

	if v, ok := m["instance_count"]; ok {
		config.InstanceCount = aws.Int64(int64(v.(int)))
	}
	if v, ok := m["instance_type"]; ok {
		config.InstanceType = aws.String(v.(string))
	}

	if v, ok := m["zone_awareness_enabled"]; ok {
		config.ZoneAwarenessEnabled = aws.Bool(v.(bool))
	}

	log.Printf("[DEBUG] Returning expanded ES Cluster Config: %s", config)

	return &config
}

func flattenESClusterConfig(c *elasticsearch.ElasticsearchClusterConfig) []map[string]interface{} {
	m := map[string]interface{}{}

	log.Printf("[DEBUG] Flattening ES Cluster Config: %s", c)

	if c.DedicatedMasterCount != nil {
		m["dedicated_master_count"] = *c.DedicatedMasterCount
	}
	if c.DedicatedMasterEnabled != nil {
		m["dedicated_master_enabled"] = *c.DedicatedMasterEnabled
	}
	if c.DedicatedMasterType != nil {
		m["dedicated_master_type"] = *c.DedicatedMasterType
	}
	if c.InstanceCount != nil {
		m["instance_count"] = *c.InstanceCount
	}
	if c.InstanceType != nil {
		m["instance_type"] = *c.InstanceType
	}
	if c.ZoneAwarenessEnabled != nil {
		m["zone_awareness_enabled"] = *c.ZoneAwarenessEnabled
	}

	log.Printf("[DEBUG] Returning flattened ES Cluster Config: %#v", m)

	return []map[string]interface{}{m}
}

func flattenESEBSOptions(o *elasticsearch.EBSOptions) []map[string]interface{} {
	m := map[string]interface{}{}

	log.Printf("[DEBUG] Flattening ES EBS Options: %s", o)

	if o.EBSEnabled != nil {
		m["ebs_enabled"] = *o.EBSEnabled
	}
	if o.Iops != nil {
		m["iops"] = *o.Iops
	}
	if o.VolumeSize != nil {
		m["volume_size"] = *o.VolumeSize
	}
	if o.VolumeType != nil {
		m["volume_type"] = *o.VolumeType
	}

	log.Printf("[DEBUG] Returning flattened ES EBS Options: %#v", m)

	return []map[string]interface{}{m}
}

func expandESEBSOptions(m map[string]interface{}) *elasticsearch.EBSOptions {
	options := elasticsearch.EBSOptions{}

	log.Printf("[DEBUG] Expanding ES EBS Options: %#v", m)

	if v, ok := m["ebs_enabled"]; ok {
		options.EBSEnabled = aws.Bool(v.(bool))
	}
	if v, ok := m["iops"]; ok && v.(int) > 0 {
		options.Iops = aws.Int64(int64(v.(int)))
	}
	if v, ok := m["volume_size"]; ok && v.(int) > 0 {
		options.VolumeSize = aws.Int64(int64(v.(int)))
	}
	if v, ok := m["volume_type"]; ok && v.(string) != "" {
		options.VolumeType = aws.String(v.(string))
	}

	log.Printf("[DEBUG] Returning expanded ES EBS Options: %s", options)

	return &options
}
