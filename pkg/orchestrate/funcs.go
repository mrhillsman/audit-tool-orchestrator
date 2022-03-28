package orchestrate

import (
	"context"
	"fmt"
	"github.com/mcuadros/go-version"
	hivev1api "github.com/openshift/hive/apis/hive/v1"
	hivev1client "github.com/openshift/hive/pkg/client/clientset/versioned"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"strings"
	"time"
)

func GetHiveClient() *hivev1client.Clientset {
	// create hive client
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("OPENSHIFT_KUBECONFIG"))
	if err != nil {
		log.Printf("Unable to build config from flags: %v\n", err)
	}

	hiveclient, err := hivev1client.NewForConfig(cfg)

	return hiveclient
}

func GetK8sClient() *kubernetes.Clientset {
	// create k8s client
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("OPENSHIFT_KUBECONFIG"))
	if err != nil {
		log.Errorf("Unable to build config from flags: %v\n", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)

	return clientset
}

func K8sClientForAudit(kubeconfig []byte) *kubernetes.Clientset {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		log.Errorf("Unable to build config from kubeconfig: %v\n", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)

	return clientset
}

func GetOpenShiftVersions(flags PoolFlags) string {
	ctx := context.Background()
	clusterImageSets, err := GetHiveClient().HiveV1().ClusterImageSets().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Unable to get ClusterImageSets: %v\n", err)
	}

	log.Info(clusterImageSets.Items)
	var cisNames []string

	for _, cis := range clusterImageSets.Items {
		cisNames = append(cisNames, strings.Split(cis.Name, "-")[1])
	}

	version.Sort(cisNames)

	var selectedVersion []string
	for _, v := range cisNames {
		if strings.Contains(v, flags.OpenShift) {
			selectedVersion = append(selectedVersion, v)
		}
	}

	return selectedVersion[len(selectedVersion)-1]
}

func WaitForSuccessfulClusterClaim(hvclient *hivev1client.Clientset, claim *hivev1api.ClusterClaim) (string, error) {
	ctx := context.Background()
	selector := fields.SelectorFromSet(map[string]string{"metadata.name": claim.Name})
	var wi watch.Interface

	err := wait.ExponentialBackoff(
		wait.Backoff{Steps: 10, Duration: 10 * time.Second, Factor: 2},
		func() (bool, error) {
			var err error
			cci := hvclient.HiveV1().ClusterClaims(claim.Namespace)

			wi, err = cci.Watch(ctx, metav1.ListOptions{FieldSelector: selector.String()})
			if err != nil {
				log.Error(err)
				return false, nil
			}

			return true, nil
		},
	)

	if err != nil {
		log.WithError(err).Fatal("failed to create watch for ClusterClaim")
	}

	for event := range wi.ResultChan() {
		clusterClaim, ok := event.Object.(*hivev1api.ClusterClaim)
		if !ok {
			log.WithField("object-type", fmt.Sprintf("%T", event.Object)).Fatal("received an unexpected object from Watch")
		}

		log.Infof("ClusterClaim event received: %v\n", clusterClaim.Status.Conditions)

		var pendingStatus, clusterRunningStatus corev1.ConditionStatus

		for _, clusterClaimCondition := range clusterClaim.Status.Conditions {
			if clusterClaimCondition.Type == "Pending" {
				pendingStatus = clusterClaimCondition.Status
			}

			if clusterClaimCondition.Type == "ClusterRunning" {
				clusterRunningStatus = clusterClaimCondition.Status
			}
		}

		if pendingStatus == "False" && clusterRunningStatus == "True" {
			watchedClaim, err := hvclient.HiveV1().ClusterClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			if err != nil {
				log.Errorf("Unable to get the ClusterClaim under watch: %v\n", err)
			}
			return watchedClaim.Spec.Namespace, nil
		}
	}

	return "", nil
}

func (c ClusterClaimDeleteFlagSetNameFlagEmptyError) Error() string {
	return "--name flag set to an existing ClusterClaim required to perform a deletion."
}

func (c ClusterClaimNameLengthIncorrectError) Error() string {
	return "--name length is incorrect; must be at least 8 but not more than 16 ASCII alphanumeric characters."
}

func (c ClusterClaimNameHasInvalidCharactersError) Error() string {
	return "--name contains invalid characters; ASCII alphanumeric characters only permitted."
}
