// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package rds

import (
	"context"

	"github.com/YakDriver/regexache"
	rds_sdkv2 "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/hashicorp/aws-sdk-go-base/v2/awsv1shim/v2/tfawserr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-provider-aws/internal/errs"
	tfslices "github.com/hashicorp/terraform-provider-aws/internal/slices"
	"github.com/hashicorp/terraform-provider-aws/internal/tfresource"
)

// NOTE ON "ID", "IDENTIFIER":
// ID is overloaded and potentially confusing. Hopefully this clears it up.
// * ID, as in d.Id(), d.SetId(), is:
//    - the same as AWS calls the "dbi-resource-id" a/k/a "database instance resource ID"
//    - unchangeable/immutable
//    - called either "id" or "resource_id" in schema/state (previously was only "resource_id")
// * "identifier" is:
//    - user-defined identifier which AWS calls "identifier"
//    - can be updated
//    - called "identifier" in the schema/state (previously was also "id")

// findDBInstanceByIDSDKv1 in general should be called with a DbiResourceId of the form
// "db-BE6UI2KLPQP3OVDYD74ZEV6NUM" rather than a DB identifier. However, in some cases only
// the identifier is available, and can be used.
func findDBInstanceByIDSDKv1(ctx context.Context, conn *rds.RDS, id string) (*rds.DBInstance, error) {
	idLooksLikeDbiResourceId := regexache.MustCompile(`^db-[0-9A-Za-z]{2,255}$`).MatchString(id)
	input := &rds.DescribeDBInstancesInput{}

	if idLooksLikeDbiResourceId {
		input.Filters = []*rds.Filter{
			{
				Name:   aws.String("dbi-resource-id"),
				Values: aws.StringSlice([]string{id}),
			},
		}
	} else {
		input.DBInstanceIdentifier = aws.String(id)
	}

	output, err := findDBInstanceSDKv1(ctx, conn, input, tfslices.PredicateTrue[*rds.DBInstance]())

	// in case a DB has an *identifier* starting with "db-""
	if idLooksLikeDbiResourceId && tfresource.NotFound(err) {
		input := &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(id),
		}

		output, err = findDBInstanceSDKv1(ctx, conn, input, tfslices.PredicateTrue[*rds.DBInstance]())
	}

	if err != nil {
		return nil, err
	}

	return output, nil
}

func findDBInstanceSDKv1(ctx context.Context, conn *rds.RDS, input *rds.DescribeDBInstancesInput, filter tfslices.Predicate[*rds.DBInstance]) (*rds.DBInstance, error) {
	output, err := findDBInstancesSDKv1(ctx, conn, input, filter)

	if err != nil {
		return nil, err
	}

	return tfresource.AssertSinglePtrResult(output)
}

func findDBInstancesSDKv1(ctx context.Context, conn *rds.RDS, input *rds.DescribeDBInstancesInput, filter tfslices.Predicate[*rds.DBInstance]) ([]*rds.DBInstance, error) {
	var output []*rds.DBInstance

	err := conn.DescribeDBInstancesPagesWithContext(ctx, input, func(page *rds.DescribeDBInstancesOutput, lastPage bool) bool {
		if page == nil {
			return !lastPage
		}

		for _, v := range page.DBInstances {
			if v != nil && filter(v) {
				output = append(output, v)
			}
		}

		return !lastPage
	})

	if tfawserr.ErrCodeEquals(err, rds.ErrCodeDBInstanceNotFoundFault) {
		return nil, &retry.NotFoundError{
			LastError:   err,
			LastRequest: input,
		}
	}

	if err != nil {
		return nil, err
	}

	return output, nil
}

// findDBInstanceByIDSDKv2 in general should be called with a DbiResourceId of the form
// "db-BE6UI2KLPQP3OVDYD74ZEV6NUM" rather than a DB identifier. However, in some cases only
// the identifier is available, and can be used.
func findDBInstanceByIDSDKv2(ctx context.Context, conn *rds_sdkv2.Client, id string) (*types.DBInstance, error) {
	input := &rds_sdkv2.DescribeDBInstancesInput{}

	if regexache.MustCompile(`^db-[0-9A-Za-z]{2,255}$`).MatchString(id) {
		input.Filters = []types.Filter{
			{
				Name:   aws.String("dbi-resource-id"),
				Values: []string{id},
			},
		}
	} else {
		input.DBInstanceIdentifier = aws.String(id)
	}

	output, err := conn.DescribeDBInstances(ctx, input)

	// in case a DB has an *identifier* starting with "db-""
	if regexache.MustCompile(`^db-[0-9A-Za-z]{2,255}$`).MatchString(id) && (output == nil || len(output.DBInstances) == 0) {
		input = &rds_sdkv2.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(id),
		}
		output, err = conn.DescribeDBInstances(ctx, input)
	}

	if errs.IsA[*types.DBInstanceNotFoundFault](err) {
		return nil, &retry.NotFoundError{
			LastError:   err,
			LastRequest: input,
		}
	}

	if err != nil {
		return nil, err
	}

	if output == nil {
		return nil, tfresource.NewEmptyResultError(input)
	}

	return tfresource.AssertSingleValueResult(output.DBInstances)
}
