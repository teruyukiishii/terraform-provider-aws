// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package rds

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/hashicorp/aws-sdk-go-base/v2/awsv1shim/v2/tfawserr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	tfslices "github.com/hashicorp/terraform-provider-aws/internal/slices"
	"github.com/hashicorp/terraform-provider-aws/internal/tfresource"
)

func FindDBClusterByID(ctx context.Context, conn *rds.RDS, id string) (*rds.DBCluster, error) {
	input := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(id),
	}
	output, err := findDBCluster(ctx, conn, input, tfslices.PredicateTrue[*rds.DBCluster]())

	if err != nil {
		return nil, err
	}

	// Eventual consistency check.
	if arn.IsARN(id) {
		if aws.StringValue(output.DBClusterArn) != id {
			return nil, &retry.NotFoundError{
				LastRequest: input,
			}
		}
	} else if aws.StringValue(output.DBClusterIdentifier) != id {
		return nil, &retry.NotFoundError{
			LastRequest: input,
		}
	}

	return output, nil
}

func findDBCluster(ctx context.Context, conn *rds.RDS, input *rds.DescribeDBClustersInput, filter tfslices.Predicate[*rds.DBCluster]) (*rds.DBCluster, error) {
	output, err := findDBClusters(ctx, conn, input, filter)

	if err != nil {
		return nil, err
	}

	return tfresource.AssertSinglePtrResult(output)
}

func findDBClusters(ctx context.Context, conn *rds.RDS, input *rds.DescribeDBClustersInput, filter tfslices.Predicate[*rds.DBCluster]) ([]*rds.DBCluster, error) {
	var output []*rds.DBCluster

	err := conn.DescribeDBClustersPagesWithContext(ctx, input, func(page *rds.DescribeDBClustersOutput, lastPage bool) bool {
		if page == nil {
			return !lastPage
		}

		for _, v := range page.DBClusters {
			if v != nil && filter(v) {
				output = append(output, v)
			}
		}

		return !lastPage
	})

	if tfawserr.ErrCodeEquals(err, rds.ErrCodeDBClusterNotFoundFault) {
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
