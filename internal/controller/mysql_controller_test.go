package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

var _ = Describe("MySQL controller", func() {
	const (
		timeout  = time.Second * 20
		interval = time.Millisecond * 250
	)

	Context("When creating a standalone MySQL CR", func() {
		It("should create secrets, services, configmap, and StatefulSet", func() {
			nsName := "mysql-ctrl-test"
			createNamespace(nsName)

			mysql := &mysqlv1alpha1.MySQL{
				ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: nsName},
				Spec: mysqlv1alpha1.MySQLSpec{
					Image:       "mysql:8.0",
					StorageSize: "1Gi",
					Database:    "appdb",
					Port:        3306,
				},
			}
			Expect(k8sClient.Create(ctx, mysql)).To(Succeed())

			secret := &corev1.Secret{}
			waitForObject(ctx, types.NamespacedName{Name: "demo-root", Namespace: nsName}, secret, timeout)
			Expect(secret.Data).To(HaveKey("password"))

			for _, name := range []string{"demo", "demo-primary", "demo-reads", "demo-headless"} {
				svc := &corev1.Service{}
				waitForObject(ctx, types.NamespacedName{Name: name, Namespace: nsName}, svc, timeout)
			}

			cm := &corev1.ConfigMap{}
			waitForObject(ctx, types.NamespacedName{Name: "demo-operator", Namespace: nsName}, cm, timeout)
			Expect(cm.Data).To(HaveKey("entrypoint.sh"))

			sts := &appsv1.StatefulSet{}
			waitForObject(ctx, types.NamespacedName{Name: "demo", Namespace: nsName}, sts, timeout)
			Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
			Expect(sts.Spec.ServiceName).To(Equal("demo-headless"))
			Expect(sts.Spec.Template.Spec.Containers[0].Command).To(ContainElement(ContainSubstring("entrypoint.sh")))

			Eventually(func() string {
				updated := &mysqlv1alpha1.MySQL{}
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "demo", Namespace: nsName}, updated)
				return updated.Status.Mode
			}, timeout, interval).Should(Equal("Standalone"))
		})
	})

	Context("When creating an HA MySQL CR", func() {
		It("should scale StatefulSet and create replication secret", func() {
			nsName := "mysql-ctrl-ha"
			createNamespace(nsName)

			var replicas int32 = 3
			mysql := &mysqlv1alpha1.MySQL{
				ObjectMeta: metav1.ObjectMeta{Name: "ha", Namespace: nsName},
				Spec: mysqlv1alpha1.MySQLSpec{
					Replicas:    &replicas,
					StorageSize: "1Gi",
				},
			}
			Expect(k8sClient.Create(ctx, mysql)).To(Succeed())

			repl := &corev1.Secret{}
			waitForObject(ctx, types.NamespacedName{Name: "ha-replication", Namespace: nsName}, repl, timeout)
			Expect(repl.Data).To(HaveKey("password"))

			sts := &appsv1.StatefulSet{}
			waitForObject(ctx, types.NamespacedName{Name: "ha", Namespace: nsName}, sts, timeout)
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.OrderedReadyPodManagement))

			// Primary service selects role=primary
			primary := &corev1.Service{}
			waitForObject(ctx, types.NamespacedName{Name: "ha-primary", Namespace: nsName}, primary, timeout)
			Expect(primary.Spec.Selector[labelRoleKey]).To(Equal(rolePrimary))

			Eventually(func() string {
				updated := &mysqlv1alpha1.MySQL{}
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "ha", Namespace: nsName}, updated)
				return updated.Status.Mode
			}, timeout, interval).Should(Equal("PrimaryReplica"))
		})
	})

	Context("When using an existing root secret", func() {
		It("should wire that secret into the StatefulSet", func() {
			nsName := "mysql-ctrl-secret-ref"
			createNamespace(nsName)

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "custom-root", Namespace: nsName},
				StringData: map[string]string{"password": "s3cret-pass"},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			mysql := &mysqlv1alpha1.MySQL{
				ObjectMeta: metav1.ObjectMeta{Name: "secured", Namespace: nsName},
				Spec: mysqlv1alpha1.MySQLSpec{
					StorageSize: "1Gi",
					RootPasswordSecretRef: &mysqlv1alpha1.SecretKeySelector{
						Name: "custom-root", Key: "password",
					},
				},
			}
			Expect(k8sClient.Create(ctx, mysql)).To(Succeed())

			sts := &appsv1.StatefulSet{}
			waitForObject(ctx, types.NamespacedName{Name: "secured", Namespace: nsName}, sts, timeout)
			var secretRefName string
			for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
				if e.Name == "MYSQL_ROOT_PASSWORD" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
					secretRefName = e.ValueFrom.SecretKeyRef.Name
				}
			}
			Expect(secretRefName).To(Equal("custom-root"))
		})
	})
})
