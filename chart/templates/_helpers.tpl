{{/* Chart name, overridable with nameOverride. */}}
{{- define "backup-controller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Release-qualified name. Defaults to the release name when it already contains
the chart name, otherwise release-chart. fullnameOverride wins outright.
*/}}
{{- define "backup-controller.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Chart identifier for the helm.sh/chart label. */}}
{{- define "backup-controller.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Selector labels shared by the Deployment, its pods, and every object. */}}
{{- define "backup-controller.selectorLabels" -}}
app.kubernetes.io/name: {{ include "backup-controller.name" . }}
app.kubernetes.io/instance: {{ include "backup-controller.fullname" . }}
{{- end -}}

{{/* Metadata labels. */}}
{{- define "backup-controller.labels" -}}
helm.sh/chart: {{ include "backup-controller.chart" . }}
{{ include "backup-controller.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Name of the ServiceAccount the Deployment runs with. */}}
{{- define "backup-controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.name -}}
{{- .Values.serviceAccount.name -}}
{{- else -}}
{{- include "backup-controller.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
The image reference: digest first, then tag, then v<appVersion>. The v prefix
matches the tag the release pipeline pushes (v1.2.3 next to the rolling 1.2
minor tag), so the fallback names a published tag.
*/}}
{{- define "backup-controller.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default (printf "v%s" .Chart.AppVersion) }}
{{- end -}}
{{- end -}}
