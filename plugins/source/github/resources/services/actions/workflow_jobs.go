package actions

import (
	"context"

	"github.com/cloudquery/cloudquery/plugins/source/github/client"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/transformers"
	"github.com/google/go-github/v49/github"
)

func WorkflowJobRuns() *schema.Table {
	return &schema.Table{
		Name:      "github_workflow_jobs",
		Resolver:  fetchWorkflowJobRuns,
		Multiplex: client.OrgRepositoryMultiplex,
		Transform: client.TransformWithStruct(&github.WorkflowJob{}, transformers.WithPrimaryKeys("ID")),
		Columns: []schema.Column{
			client.OrgColumn,
			client.RepositoryIDColumn,
		},
		Parent: (WorkflowRuns()),
	}
}

func fetchWorkflowJobRuns(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- any) error {
	c := meta.(*client.Client)
	repo := c.Repository
	workflowRun := parent.Item.(github.WorkflowRun)
	actionOpts := &github.ListWorkflowJobsOptions{Filter: "all", ListOptions: github.ListOptions{PerPage: 100}}
	for {
		workflows, resp, err := c.Github.Actions.ListWorkflowJobs(ctx, *repo.Owner.Login, *repo.Name, *workflowRun.WorkflowID, actionOpts)
		if err != nil {
			return err
		}
		res <- workflows.Jobs

		if resp.NextPage == 0 {
			break
		}
		actionOpts.Page = resp.NextPage
	}
	return nil
}
