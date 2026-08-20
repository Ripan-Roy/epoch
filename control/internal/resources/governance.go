package resources

import (
	"fmt"
	"strings"
)

const (
	maxGovernanceOwnerBytes      = 128
	maxGovernanceCostCenterBytes = 64
	maxGovernanceTags            = 32
	maxGovernanceTagKeyBytes     = 63
	maxGovernanceTagValueBytes   = 256
	reservedGovernanceTagPrefix  = "epoch.io/"
)

// DataClassification is the stable non-secret governance taxonomy exposed by
// the managed API and persisted in both management and regional catalogs.
type DataClassification string

const (
	ClassificationUnspecified  DataClassification = "unspecified"
	ClassificationPublic       DataClassification = "public"
	ClassificationInternal     DataClassification = "internal"
	ClassificationConfidential DataClassification = "confidential"
	ClassificationRestricted   DataClassification = "restricted"
)

// Valid reports whether the classification is assignable to a managed
// resource. Unspecified is reserved for legacy inventory and query omission.
func (classification DataClassification) Valid() bool {
	switch classification {
	case ClassificationPublic, ClassificationInternal,
		ClassificationConfidential, ClassificationRestricted:
		return true
	default:
		return false
	}
}

// ResourceGovernance contains bounded ownership, chargeback, and
// classification metadata. Environment remains authoritative in ResourceKey.
type ResourceGovernance struct {
	Owner          string             `json:"owner"`
	CostCenter     string             `json:"cost_center"`
	Classification DataClassification `json:"classification"`
	Tags           map[string]string  `json:"tags"`
}

// NormalizeGovernance returns a detached canonical value. A nil value is kept
// for legacy catalog recovery; public RegionalAdmin creation requires one.
func NormalizeGovernance(governance *ResourceGovernance) (*ResourceGovernance, error) {
	if governance == nil {
		return nil, nil
	}
	owner, err := normalizeGovernanceIdentifier(
		"governance owner",
		governance.Owner,
		maxGovernanceOwnerBytes,
	)
	if err != nil {
		return nil, err
	}
	costCenter, err := normalizeGovernanceIdentifier(
		"governance cost center",
		governance.CostCenter,
		maxGovernanceCostCenterBytes,
	)
	if err != nil {
		return nil, err
	}
	classification := DataClassification(strings.ToLower(strings.TrimSpace(string(governance.Classification))))
	if !classification.Valid() {
		return nil, invalid("governance classification must be public, internal, confidential, or restricted")
	}
	tags, err := normalizeGovernanceTags(governance.Tags)
	if err != nil {
		return nil, err
	}
	return &ResourceGovernance{
		Owner:          owner,
		CostCenter:     costCenter,
		Classification: classification,
		Tags:           tags,
	}, nil
}

func normalizeGovernanceFilter(filter ListFilter) (ListFilter, error) {
	var err error
	if strings.TrimSpace(filter.Owner) != "" {
		filter.Owner, err = normalizeGovernanceIdentifier(
			"governance owner filter",
			filter.Owner,
			maxGovernanceOwnerBytes,
		)
		if err != nil {
			return ListFilter{}, err
		}
	}
	if strings.TrimSpace(filter.CostCenter) != "" {
		filter.CostCenter, err = normalizeGovernanceIdentifier(
			"governance cost center filter",
			filter.CostCenter,
			maxGovernanceCostCenterBytes,
		)
		if err != nil {
			return ListFilter{}, err
		}
	}
	if filter.Classification != "" {
		filter.Classification = DataClassification(
			strings.ToLower(strings.TrimSpace(string(filter.Classification))),
		)
		if !filter.Classification.Valid() {
			return ListFilter{}, invalid(
				"classification filter must be public, internal, confidential, or restricted",
			)
		}
	}
	filter.Tags, err = normalizeGovernanceTags(filter.Tags)
	if err != nil {
		return ListFilter{}, err
	}
	return filter, nil
}

func normalizeGovernanceIdentifier(label, value string, maximum int) (string, error) {
	canonical := strings.ToLower(strings.TrimSpace(value))
	if canonical == "" {
		return "", invalid(label + " is required")
	}
	if len(canonical) > maximum {
		return "", invalid(fmt.Sprintf("%s must be at most %d bytes", label, maximum))
	}
	for index := range len(canonical) {
		character := canonical[index]
		if !isGovernanceIdentifierByte(character) {
			return "", invalid(label + " must contain only lowercase letters, numbers, '.', '_', ':', '@', '/', or '-'")
		}
	}
	if !isASCIIAlphanumeric(canonical[0]) {
		return "", invalid(label + " must begin with a letter or number")
	}
	return canonical, nil
}

func normalizeGovernanceTags(tags map[string]string) (map[string]string, error) {
	if len(tags) > maxGovernanceTags {
		return nil, invalid(fmt.Sprintf("governance supports at most %d tags", maxGovernanceTags))
	}
	if len(tags) == 0 {
		return nil, nil
	}
	normalized := make(map[string]string, len(tags))
	for key, value := range tags {
		canonicalKey, err := normalizeGovernanceIdentifier(
			"governance tag key",
			key,
			maxGovernanceTagKeyBytes,
		)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(canonicalKey, reservedGovernanceTagPrefix) {
			return nil, invalid("governance tag prefix " + reservedGovernanceTagPrefix + " is reserved")
		}
		if _, duplicate := normalized[canonicalKey]; duplicate {
			return nil, invalid("governance tag keys must be unique after canonicalization")
		}
		canonicalValue := strings.TrimSpace(value)
		if canonicalValue == "" {
			return nil, invalid("governance tag values must be non-empty")
		}
		if len(canonicalValue) > maxGovernanceTagValueBytes {
			return nil, invalid(fmt.Sprintf(
				"governance tag values must be at most %d bytes",
				maxGovernanceTagValueBytes,
			))
		}
		if strings.IndexFunc(canonicalValue, func(character rune) bool {
			return character < 0x20 || character == 0x7f
		}) >= 0 {
			return nil, invalid("governance tag values cannot contain control characters")
		}
		normalized[canonicalKey] = canonicalValue
	}
	return normalized, nil
}

func isGovernanceIdentifierByte(character byte) bool {
	return isASCIIAlphanumeric(character) ||
		character == '.' || character == '_' || character == ':' ||
		character == '@' || character == '/' || character == '-'
}

func isASCIIAlphanumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func cloneGovernance(governance *ResourceGovernance) *ResourceGovernance {
	if governance == nil {
		return nil
	}
	cloned := *governance
	cloned.Tags = cloneLabels(governance.Tags)
	return &cloned
}

func governanceEqual(left, right *ResourceGovernance) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.Owner != right.Owner ||
		left.CostCenter != right.CostCenter ||
		left.Classification != right.Classification ||
		len(left.Tags) != len(right.Tags) {
		return false
	}
	for key, value := range left.Tags {
		if right.Tags[key] != value {
			return false
		}
	}
	return true
}

func governanceMatches(governance *ResourceGovernance, filter ListFilter) bool {
	if filter.Owner == "" && filter.CostCenter == "" &&
		filter.Classification == "" && len(filter.Tags) == 0 {
		return true
	}
	if governance == nil {
		return false
	}
	if filter.Owner != "" && governance.Owner != filter.Owner ||
		filter.CostCenter != "" && governance.CostCenter != filter.CostCenter ||
		filter.Classification != "" && governance.Classification != filter.Classification {
		return false
	}
	for key, value := range filter.Tags {
		if governance.Tags[key] != value {
			return false
		}
	}
	return true
}
