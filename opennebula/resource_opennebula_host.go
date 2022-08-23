package opennebula

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	dyn "github.com/OpenNebula/one/src/oca/go/src/goca/dynamic"
	"github.com/OpenNebula/one/src/oca/go/src/goca/parameters"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/host"
)

var hostTypes = []string{"KVM", "QEMU", "LXD", "LXC", "FIRECRACKER", "VCENTER", "CUSTOM"}
var defaultHostMinTimeout = 20
var defaultHostTimeout = time.Duration(defaultHostMinTimeout) * time.Minute

func resourceOpennebulaHost() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceOpennebulaHostCreate,
		ReadContext:   resourceOpennebulaHostRead,
		UpdateContext: resourceOpennebulaHostUpdate,
		DeleteContext: resourceOpennebulaHostDelete,
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(defaultHostTimeout),
			Delete: schema.DefaultTimeout(defaultHostTimeout),
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the Host",
			},
			"type": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Type of the new host: kvm, qemu, lxd, lxc, firecracker, vcenter, custom",
				ValidateFunc: func(v interface{}, k string) (ws []string, errors []error) {
					value := strings.ToUpper(v.(string))

					if inArray(value, hostTypes) < 0 {
						errors = append(errors, fmt.Errorf("host \"type\" must be one of: %s", strings.Join(hostTypes, ",")))
					}

					return
				},
			},
			/*
				"vcenter": {
					Type:     schema.TypeSet,
					Optional: true,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"user": {
								Type:        schema.TypeString,
								Required:    true,
								Description: "VCenter user name",
							},
							"password": {
								Type:        schema.TypeString,
								Required:    true,
								Description: "VCenter user password",
							},
						},
					},
					ConflictsWith: []string{"custom"},
				},
			*/
			"custom": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"virtualization": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "Virtualization driver",
						},
						"information": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "Information driver",
						},
					},
				},
				//ConflictsWith: []string{"vsphere"},
			},
			"overcommit": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"cpu": {
							Type:        schema.TypeInt,
							Required:    true,
							Description: "Maximum allocatable CPU capacity in number of cores multiplied by 100",
						},
						"memory": {
							Type:        schema.TypeInt,
							Required:    true,
							Description: "Maximum allocatable memory capacity in KB",
						},
					},
				},
			},
			"cluster_id": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     -1,
				Description: "ID of the cluster",
			},
			"tags":         tagsSchema(),
			"default_tags": defaultTagsSchemaComputed(),
			"tags_all":     tagsSchemaComputed(),
		},
	}
}

func getHostController(d *schema.ResourceData, meta interface{}) (*goca.HostController, error) {
	config := meta.(*Configuration)
	controller := config.Controller

	uid, err := strconv.ParseUint(d.Id(), 10, 0)
	if err != nil {
		return nil, err
	}

	return controller.Host(int(uid)), nil
}

func resourceOpennebulaHostCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	config := meta.(*Configuration)
	controller := config.Controller

	var diags diag.Diagnostics

	name := d.Get("name").(string)
	hostType := strings.ToUpper(d.Get("type").(string))

	var vmMad, imMad string

	switch hostType {
	case "KVM", "QEMU", "LXD", "LXC", "FIRECRACKER", "VCENTER":
		imMad = hostType
		vmMad = hostType
		/*
			case "VCENTER":
				//accountList := d.Get("vcenter").(*schema.Set).List()

				imMad = "VCENTER"
				vmMad = "VCENTER"


					for _, accountIf := range accountList {
						accountMap := accountIf.(map[string]interface{})
						user, ok := accountMap["user"]
						if !ok {
						}
						tpl.AddPair()
						passwordIf, ok := accountMap["password"]
						if !ok {
						}
						password = passwordIf.(string)

					}

		*/
	case "CUSTOM":

		madsList := d.Get("custom").(*schema.Set).List()

		for _, madsIf := range madsList {
			madsMap := madsIf.(map[string]interface{})
			imMadIf, ok := madsMap["information"]
			if !ok {
				diags = append(diags, diag.Diagnostic{
					Severity: diag.Error,
					Summary:  "No information field found in the custom section",
				})
				return diags
			}
			imMad = imMadIf.(string)
			vmMadIf, ok := madsMap["virtualization"]
			if !ok {
				diags = append(diags, diag.Diagnostic{
					Severity: diag.Error,
					Summary:  "No virtualization field found in the custom section",
				})
				return diags
			}
			vmMad = vmMadIf.(string)

		}

	}

	clusterID := d.Get("cluster_id").(int)

	hostID, err := controller.Hosts().Create(name, imMad, vmMad, clusterID)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to create the host",
			Detail:   err.Error(),
		})
		return diags
	}
	d.SetId(fmt.Sprintf("%v", hostID))

	log.Printf("[INFO] Host created")

	hc := controller.Host(hostID)

	timeout := d.Timeout(schema.TimeoutCreate)
	_, err = waitForHostState(ctx, hc, timeout, "MONITORED")
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to wait host to be in MONITORED state",
			Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
		})
		return diags
	}

	hostTpl, err := generateHostTemplate(d, meta, hc)

	hostTplStr := hostTpl.String()
	log.Printf("[INFO] Host template: %s", hostTplStr)

	err = hc.Update(hostTplStr, parameters.Replace)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to retrieve information",
			Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
		})
		return diags
	}

	return resourceOpennebulaHostRead(ctx, d, meta)
}

func generateHostTemplate(d *schema.ResourceData, meta interface{}, hc *goca.HostController) (*dyn.Template, error) {

	config := meta.(*Configuration)
	tpl := dyn.NewTemplate()

	err := generateHostOvercommit(d, meta, hc, tpl)
	if err != nil {
		return nil, err
	}

	tagsInterface := d.Get("tags").(map[string]interface{})
	for k, v := range tagsInterface {
		tpl.AddPair(strings.ToUpper(k), v)
	}

	// add default tags if they aren't overriden
	if len(config.defaultTags) > 0 {
		for k, v := range config.defaultTags {
			key := strings.ToUpper(k)
			p, _ := tpl.GetPair(key)
			if p != nil {
				continue
			}
			tpl.AddPair(key, v)
		}
	}

	return tpl, nil
}

func generateHostOvercommit(d *schema.ResourceData, meta interface{}, hc *goca.HostController, tpl *dyn.Template) error {

	overcommit, ok := d.GetOk("overcommit")
	if ok {
		overcommitList := overcommit.(*schema.Set).List()

		hostInfos, err := hc.Info(false)
		if err != nil {
			return fmt.Errorf("Failed to retrieve informations")
		}

		for _, overcommitIf := range overcommitList {
			overcommitMap := overcommitIf.(map[string]interface{})
			cpuIf, ok := overcommitMap["cpu"]
			if !ok {
				return fmt.Errorf("No cpu field found in the overcommit section")
			}
			memoryIf, ok := overcommitMap["memory"]
			if !ok {
				return fmt.Errorf("No memory field found in the overcommit section")
			}

			reservedCPU := cpuIf.(int) - hostInfos.Share.TotalCPU
			reservedMem := memoryIf.(int) - hostInfos.Share.TotalMem

			tpl.AddPair("RESERVED_CPU", reservedCPU)
			tpl.AddPair("RESERVED_MEMORY", reservedMem)
		}
	}

	return nil
}

func resourceOpennebulaHostRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {

	var diags diag.Diagnostics
	config := meta.(*Configuration)

	hc, err := getHostController(d, meta)
	if err != nil {
		if NoExists(err) {
			log.Printf("[WARN] Removing host %s from state because it no longer exists in", d.Get("name"))
			d.SetId("")
			return nil
		}

		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to get the host controller",
			Detail:   err.Error(),
		})
		return diags

	}

	host, err := hc.Info(false)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to retrieve informations",
			Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
		})
		return diags
	}

	d.SetId(fmt.Sprintf("%v", host.ID))
	d.Set("name", host.Name)

	tags := make(map[string]interface{})
	tagsAll := make(map[string]interface{})

	/*
		madsList := d.Get("custom").(*schema.Set).List()
		if len(madsList) > 0 {
			information, err := host.Template.Get(hostk.IMMAD)
			if err == nil {
				d.Set("dev_prefix", information)
			}
			virtualization, err := host.Template.Get(hostk.VMMAD)
			if err == nil {
				d.Set("driver", virtualization)
			}
		}
	*/

	// Get default tags
	oldDefault := d.Get("default_tags").(map[string]interface{})
	for k, _ := range oldDefault {
		tagValue, err := host.Template.GetStr(strings.ToUpper(k))
		if err != nil {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Failed to get default tag",
				Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
			})
			return diags
		}
		tagsAll[k] = tagValue
	}
	d.Set("default_tags", config.defaultTags)

	// Get only tags described in the configuration
	if tagsInterface, ok := d.GetOk("tags"); ok {
		for k, _ := range tagsInterface.(map[string]interface{}) {
			tagValue, err := host.Template.GetStr(strings.ToUpper(k))
			if err != nil {
				diags = append(diags, diag.Diagnostic{
					Severity: diag.Error,
					Summary:  "Failed to get tag from the host template",
					Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
				})
				return diags

			}
			tags[k] = tagValue
			tagsAll[k] = tagValue
		}

		err := d.Set("tags", tags)
		if err != nil {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Failed to set attribute",
				Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
			})
			return diags
		}
	}
	d.Set("tags_all", tagsAll)

	return nil
}

func resourceOpennebulaHostUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {

	var diags diag.Diagnostics

	hc, err := getHostController(d, meta)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to get the host controller",
			Detail:   err.Error(),
		})
		return diags
	}

	hostInfos, err := hc.Info(false)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to retrieve informations",
			Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
		})
		return diags
	}

	if d.HasChange("name") {
		err := hc.Rename(d.Get("name").(string))
		if err != nil {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Failed to rename",
				Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
			})
			return diags
		}
		log.Printf("[INFO] Successfully updated name for host %s\n", hostInfos.Name)
	}

	update := false
	newTpl := hostInfos.Template

	if d.HasChange("overcommit") {
		newTpl.Del("RESERVED_CPU")
		newTpl.Del("RESERVED_MEMORY")
		err := generateHostOvercommit(d, meta, hc, &newTpl.Template)
		if err != nil {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Failed to compute host overcommit",
				Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
			})
			return diags
		}
		update = true
	}

	if d.HasChange("tags") {

		oldTagsIf, newTagsIf := d.GetChange("tags")
		oldTags := oldTagsIf.(map[string]interface{})
		newTags := newTagsIf.(map[string]interface{})

		// delete tags
		for k, _ := range oldTags {
			_, ok := newTags[k]
			if ok {
				continue
			}
			newTpl.Del(strings.ToUpper(k))
		}

		// add/update tags
		for k, v := range newTags {
			newTpl.Del(strings.ToUpper(k))
			newTpl.AddPair(strings.ToUpper(k), v)
		}

		update = true
	}

	if update {
		err = hc.Update(newTpl.String(), parameters.Replace)
		if err != nil {
			diags = append(diags, diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Failed to update content",
				Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
			})
			return diags
		}
	}

	return resourceOpennebulaHostRead(ctx, d, meta)
}

func resourceOpennebulaHostDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {

	var diags diag.Diagnostics

	hc, err := getHostController(d, meta)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to get the host controller",
			Detail:   err.Error(),
		})
		return diags
	}

	timeout := d.Timeout(schema.TimeoutDelete)
	_, err = waitForHostState(ctx, hc, timeout, "notfound")
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to wait host to be in NOTFOUND state",
			Detail:   fmt.Sprintf("host (ID: %s): %s", d.Id(), err),
		})
		return diags
	}
	return nil
}

func waitForHostState(ctx context.Context, hc *goca.HostController, timeout time.Duration, state ...string) (interface{}, error) {

	stateConf := &resource.StateChangeConf{
		Pending: []string{"anythingelse"},
		Target:  state,
		Refresh: func() (interface{}, string, error) {

			log.Println("Refreshing host state...")

			// TODO: fix it after 5.10 release
			// Force the "decrypt" bool to false to keep ONE 5.8 behavior
			hostInfos, err := hc.Info(false)
			if err != nil {
				if NoExists(err) {
					return hostInfos, "notfound", nil
				}
				return hostInfos, "", err
			}
			state, err := hostInfos.State()
			if err != nil {
				return hostInfos, "", err
			}

			log.Printf("Host (ID:%d, name:%s) is currently in state %v", hostInfos.ID, hostInfos.Name, state.String())

			switch state {
			case host.Monitored:
				return hostInfos, state.String(), nil
			case host.Error:
				return hostInfos, state.String(), fmt.Errorf("Host (ID:%d) entered error state.", hostInfos.ID)
			default:
				return hostInfos, "anythingelse", nil
			}
		},
		Timeout:    timeout,
		Delay:      10 * time.Second,
		MinTimeout: 3 * time.Second,
	}

	return stateConf.WaitForStateContext(ctx)
}
