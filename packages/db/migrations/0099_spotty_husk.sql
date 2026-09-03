ALTER TABLE "execution_host_assignment" DROP CONSTRAINT "execution_host_assignment_status_check";--> statement-breakpoint
ALTER TABLE "execution_host_assignment" DROP CONSTRAINT "execution_host_assignment_destroyed_check";--> statement-breakpoint
DROP INDEX "execution_host_assignment_active_host_idx";--> statement-breakpoint
DROP INDEX "execution_host_assignment_active_allocation_idx";--> statement-breakpoint
CREATE UNIQUE INDEX "execution_host_assignment_active_host_idx" ON "execution_host_assignment" USING btree ("host_id") WHERE "execution_host_assignment"."status" in ('claiming', 'assigned', 'releasing');--> statement-breakpoint
CREATE UNIQUE INDEX "execution_host_assignment_active_allocation_idx" ON "execution_host_assignment" USING btree ("allocation_id") WHERE "execution_host_assignment"."status" in ('claiming', 'assigned', 'releasing');--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ADD CONSTRAINT "execution_host_assignment_status_check" CHECK ("execution_host_assignment"."status" in ('claiming', 'assigned', 'releasing', 'destroyed'));--> statement-breakpoint
ALTER TABLE "execution_host_assignment" ADD CONSTRAINT "execution_host_assignment_destroyed_check" CHECK (("execution_host_assignment"."status" in ('releasing', 'destroyed')) = ("execution_host_assignment"."destroyed_generation" is not null));
