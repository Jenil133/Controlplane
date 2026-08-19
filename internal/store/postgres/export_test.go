package postgres

import "context"

// MigrateTo applies the migrations up to and including version, leaving the
// schema an older release created.
func (s *Store) MigrateTo(ctx context.Context, version string) error {
	return s.migrate(ctx, version)
}
